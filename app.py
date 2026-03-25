"""
Forge API — Standalone Flask service for the Forge freelancer mobile app.
Connects to production Supabase. Deployed on Railway.
"""

import os
import json as _json
import jwt
import bcrypt
import resend
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, g
import requests as http_requests
try:
    from google.oauth2 import service_account as gsa
    from google.auth.transport.requests import Request as GAuthRequest
    _gauth_available = True
except ImportError:
    _gauth_available = False
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=[
    "https://forge-app-sigma.vercel.app",
    "https://forge.bigops.in",
    "http://localhost:3000",
    "http://localhost:5173",
], supports_credentials=True, allow_headers=["Authorization", "Content-Type"])

JWT_SECRET = os.environ.get('FREELANCER_JWT_SECRET', 'forge-dev-secret-change-me')
JWT_EXPIRY_DAYS = 14

resend.api_key = os.environ.get('RESEND_API_KEY', '')

BRANCH_EMAILS = {
    '4207e135-96a0-483c-82d3-29430973b2ca': 'productionusa@theprintspace.com',       # US
    '1f7638fc-44d8-43a3-9a15-c9debfb19406': 'productionuk@theprintspace.co.uk',      # UK
    '1a5f8dd8-1a09-4ff6-af90-1a93f565a01f': 'productionde@theprintspace.com',        # DE
}

FORGE_APP_URL = 'https://forge-app-sigma.vercel.app'

# ── Firebase Cloud Messaging ──

_fcm_creds = None

def _get_fcm_access_token():
    """Get OAuth2 access token for FCM HTTP v1 API."""
    global _fcm_creds
    if not _gauth_available:
        return None
    if _fcm_creds is None:
        pk = os.environ.get('FIREBASE_PRIVATE_KEY', '')
        if not pk:
            return None
        info = {
            "type": "service_account",
            "project_id": os.environ.get('FIREBASE_PROJECT_ID', ''),
            "private_key": pk.replace('\\n', '\n'),
            "client_email": os.environ.get('FIREBASE_CLIENT_EMAIL', ''),
            "token_uri": "https://oauth2.googleapis.com/token",
        }
        _fcm_creds = gsa.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/firebase.messaging"]
        )
    _fcm_creds.refresh(GAuthRequest())
    return _fcm_creds.token

def send_push(fcm_token, title, body, link='/'):
    """Send a web push notification via FCM HTTP v1 API."""
    if not fcm_token or not _gauth_available:
        return
    access_token = _get_fcm_access_token()
    if not access_token:
        return
    project_id = os.environ.get('FIREBASE_PROJECT_ID', '')
    url = f"https://fcm.googleapis.com/v1/projects/{project_id}/messages:send"
    payload = {
        "message": {
            "token": fcm_token,
            "webpush": {
                "notification": {
                    "title": title,
                    "body": body,
                    "icon": "/icon-192.svg"
                },
                "fcm_options": {
                    "link": FORGE_APP_URL + link
                }
            }
        }
    }
    try:
        resp = http_requests.post(url, json=payload, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        })
        if resp.status_code != 200:
            print(f"FCM push failed ({resp.status_code}): {resp.text[:200]}")
    except Exception as e:
        print(f"Push failed: {e}")


# ── DB Connection Pool ──

_pool = None

def _get_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            2, 10,
            host=os.environ.get("SUPABASE_DB_HOST", "db.vxhyfjqpmjsxvhyuxaar.supabase.co"),
            port=int(os.environ.get("SUPABASE_DB_PORT", "5432")),
            dbname="postgres",
            user=os.environ.get("SUPABASE_DB_USER", "postgres"),
            password=os.environ.get("SUPABASE_DB_PASSWORD", ""),
            options="-c search_path=public",
        )
    return _pool

def get_conn():
    conn = _get_pool().getconn()
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    return conn

def release_conn(conn):
    try:
        _get_pool().putconn(conn)
    except Exception:
        try: conn.close()
        except Exception: pass


# ── Auth Middleware ──

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # Let CORS preflight through without auth
        if request.method == 'OPTIONS':
            return '', 204
        auth_header = request.headers.get('Authorization', '')
        if not auth_header.startswith('Bearer '):
            return jsonify({"error": "Missing or invalid Authorization header"}), 401
        token = auth_header.replace('Bearer ', '')
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=['HS256'])
            g.personnel_id = payload['personnel_id']
            g.branch_id = payload.get('branch_id')
            g.email = payload.get('email')
        except jwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"error": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated


# ── Audit Helpers ──

def _get_active_clock_entry(cur, personnel_id, today):
    """Find active clock_entry for today (if exists)."""
    cur.execute("""
        SELECT id FROM clock_entries
        WHERE personnel_id = %s AND shift_date = %s AND status = 'active'
        LIMIT 1
    """, (personnel_id, today))
    row = cur.fetchone()
    return row['id'] if row else None


def _log_event(cur, clock_entry_id, roster_entry_id, personnel_id,
               event_type, department=None, metadata=None):
    """Insert into clock_events audit log."""
    cur.execute("""
        INSERT INTO clock_events
            (clock_entry_id, roster_entry_id, personnel_id, event_type,
             timestamp, department, metadata)
        VALUES (%s, %s, %s, %s, now(), %s, %s)
    """, (clock_entry_id, roster_entry_id, personnel_id, event_type,
          department, _json.dumps(metadata or {})))


# ── Health Check ──

@app.route('/', methods=['GET'])
def health():
    return jsonify({"service": "forge-api", "status": "ok", "time": datetime.utcnow().isoformat()})


# ── CRON: Availability Reminder ──

@app.route('/cron/availability-reminder', methods=['GET'])
def cron_availability_reminder():
    """Daily cron — sends availability reminder emails 3 days before next fortnightly window."""
    import math
    from datetime import date

    today = date.today()
    base = date(2026, 3, 23)  # Known Monday window start
    days_since = (today - base).days
    if days_since < 0:
        return jsonify({"status": "skipped", "reason": "before base date"})

    next_window = base + timedelta(days=math.ceil(days_since / 14) * 14)
    gap = (next_window - today).days

    if gap != 3:
        return jsonify({"status": "skipped", "reason": f"{gap} days until next window (need 3)", "next_window": str(next_window)})

    window_start = str(next_window)
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Get all active freelancers with email, grouped by branch
        cur.execute("""
            SELECT id, full_name, email, branch_id FROM personnel
            WHERE is_active = true AND email IS NOT NULL AND personnel_type = 'freelancer'
        """)
        personnel = cur.fetchall()

        # Get template per branch
        cur.execute("""
            SELECT branch_id, setting_value FROM roster_settings
            WHERE setting_key = 'email_template_availability_reminder'
        """)
        templates = {str(r['branch_id']): r['setting_value'] for r in cur.fetchall()}

        sent = 0
        skipped = 0

        for person in personnel:
            pid = str(person['id'])
            bid = str(person['branch_id'] or '')

            # Skip if already submitted availability for this window
            cur.execute("""
                SELECT count(*) as cnt FROM freelancer_availability
                WHERE personnel_id = %s AND date >= %s
            """, (pid, window_start))
            if cur.fetchone()['cnt'] > 0:
                skipped += 1
                continue

            template = templates.get(bid)
            if not template:
                skipped += 1
                continue

            subject = (template.get('subject', 'Action Required: Your Availability')
                       .replace('[name]', (person['full_name'] or '').split(' ')[0]))
            body_text = (template.get('body', '')
                         .replace('[name]', (person['full_name'] or '').split(' ')[0]))
            cta_text = template.get('cta_text', 'Update Availability')

            branch_email = BRANCH_EMAILS.get(bid, 'productionuk@theprintspace.co.uk')

            html_body = f"""
            <div style="font-family:'Inter',Arial,sans-serif;max-width:520px;margin:0 auto;padding:32px 20px;">
                <div style="text-align:center;margin-bottom:28px;">
                    <div style="display:inline-block;background:#1E2D18;border-radius:12px;padding:12px 16px;">
                        <span style="color:#fff;font-size:22px;font-weight:700;">Forge</span>
                    </div>
                </div>
                <div style="font-size:14px;color:#5A6E50;line-height:1.6;white-space:pre-line;margin-bottom:24px;">
                    {body_text}
                </div>
                <div style="text-align:center;margin-bottom:28px;">
                    <a href="{FORGE_APP_URL}/availability" style="display:inline-block;background:#4A6838;color:#fff;
                        text-decoration:none;padding:14px 32px;border-radius:12px;font-size:15px;font-weight:600;">
                        {cta_text}
                    </a>
                </div>
                <p style="font-size:11px;color:#B8C4B0;text-align:center;">theprintspace &middot; Forge</p>
            </div>
            """

            try:
                resend.Emails.send({
                    "from": "Forge <noreply@theprintspace.com>",
                    "to": [person['email']],
                    "reply_to": branch_email,
                    "subject": subject,
                    "html": html_body,
                })
                sent += 1
            except Exception as e:
                print(f"Availability reminder email failed for {person['email']}: {e}")
                skipped += 1

        return jsonify({"status": "sent", "sent": sent, "skipped": skipped, "next_window": window_start})
    finally:
        release_conn(conn)


# ── AUTH ──

@app.route('/api/freelancer/auth/login', methods=['POST', 'OPTIONS'])
def login():
    if request.method == 'OPTIONS':
        return '', 204

    data = request.get_json()
    email = (data or {}).get('email', '').strip().lower()
    password = (data or {}).get('password', '')

    if not email or not password:
        return jsonify({"error": "Email and password required"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, full_name, email, branch_id, password_hash, pay_per_hour, currency FROM personnel "
            "WHERE email = %s AND personnel_type = 'freelancer' AND is_active = true",
            (email,)
        )
        user = cur.fetchone()

        if not user:
            return jsonify({"error": "Invalid email or password"}), 401
        if not user['password_hash']:
            return jsonify({"error": "No password set. Contact your manager."}), 401
        if not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            return jsonify({"error": "Invalid email or password"}), 401

        # Update last login timestamp
        cur.execute("UPDATE personnel SET last_login_at = now() WHERE id = %s", (user['id'],))
        conn.commit()

        token = jwt.encode({
            'personnel_id': str(user['id']),
            'email': user['email'],
            'branch_id': str(user['branch_id']) if user['branch_id'] else None,
            'role': 'freelancer',
            'exp': datetime.utcnow() + timedelta(days=JWT_EXPIRY_DAYS),
        }, JWT_SECRET, algorithm='HS256')

        return jsonify({
            "token": token,
            "user": {
                "id": str(user['id']),
                "name": user['full_name'],
                "email": user['email'],
                "pay_per_hour": float(user['pay_per_hour'] or 0),
                "currency": user['currency'] or 'GBP',
            }
        })
    finally:
        release_conn(conn)


@app.route('/api/freelancer/auth/reset-password', methods=['POST', 'OPTIONS'])
def reset_password():
    if request.method == 'OPTIONS':
        return '', 204
    return jsonify({"message": "If that email exists, a reset link has been sent."})


# ── INVITE SYSTEM ──

@app.route('/api/freelancer/admin/invite', methods=['POST', 'OPTIONS'])
def admin_invite():
    if request.method == 'OPTIONS':
        return '', 204

    data = request.get_json() or {}
    personnel_id = data.get('personnel_id')
    if not personnel_id:
        return jsonify({"error": "personnel_id required"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, full_name, email, branch_id FROM personnel
            WHERE id = %s AND is_active = true
        """, (personnel_id,))
        person = cur.fetchone()

        if not person:
            return jsonify({"error": "Personnel not found"}), 404
        if not person['email']:
            return jsonify({"error": "No email address on file"}), 400

        # Create invite token
        cur.execute("""
            INSERT INTO forge_invite_tokens (personnel_id)
            VALUES (%s)
            RETURNING token
        """, (personnel_id,))
        token = str(cur.fetchone()['token'])
        conn.commit()

        # Build email
        branch_email = BRANCH_EMAILS.get(str(person['branch_id'] or ''), 'productionuk@theprintspace.co.uk')
        setup_url = f'{FORGE_APP_URL}/set-password?token={token}'
        first_name = (person['full_name'] or '').split(' ')[0] or 'there'

        html_body = f"""
        <div style="font-family: 'Inter', Arial, sans-serif; max-width: 520px; margin: 0 auto; padding: 32px 20px;">
            <div style="text-align: center; margin-bottom: 28px;">
                <div style="display: inline-block; background: #1E2D18; border-radius: 12px; padding: 12px 16px;">
                    <span style="color: #fff; font-size: 22px; font-weight: 700; font-family: 'Epilogue', sans-serif;">Forge</span>
                </div>
            </div>

            <h1 style="font-size: 22px; font-weight: 700; color: #191C19; margin: 0 0 12px; text-align: center;">
                Welcome to Forge, {first_name}
            </h1>

            <p style="font-size: 14px; color: #5A6E50; line-height: 1.6; margin: 0 0 24px; text-align: center;">
                You've been invited to join Forge — the shift management app for theprintspace freelancers.
                Accept shifts, clock in with QR, track your earnings, and manage your availability.
            </p>

            <div style="text-align: center; margin-bottom: 28px;">
                <a href="{setup_url}" style="display: inline-block; background: #4A6838; color: #fff; text-decoration: none;
                    padding: 14px 32px; border-radius: 12px; font-size: 15px; font-weight: 600;">
                    Set up your account
                </a>
            </div>

            <p style="font-size: 12px; color: #98A890; line-height: 1.5; text-align: center; margin: 0 0 8px;">
                This link expires in 48 hours. If you didn't expect this email, you can safely ignore it.
            </p>
            <p style="font-size: 11px; color: #B8C4B0; text-align: center; margin: 0;">
                theprintspace &middot; Forge
            </p>
        </div>
        """

        # Send via Resend
        try:
            resend.Emails.send({
                "from": "Forge <noreply@theprintspace.com>",
                "to": [person['email']],
                "reply_to": branch_email,
                "cc": [branch_email],
                "subject": "Welcome to Forge | The new way to work at theprintspace",
                "html": html_body,
            })
        except Exception as e:
            return jsonify({"error": f"Email send failed: {str(e)}"}), 500

        return jsonify({"success": True})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/auth/verify-invite', methods=['POST', 'OPTIONS'])
def verify_invite():
    if request.method == 'OPTIONS':
        return '', 204

    data = request.get_json() or {}
    token = data.get('token', '')
    if not token:
        return jsonify({"error": "Token required"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT it.personnel_id, p.full_name, p.email
            FROM forge_invite_tokens it
            JOIN personnel p ON p.id = it.personnel_id
            WHERE it.token = %s AND it.used = false AND it.expires_at > now()
        """, (token,))
        row = cur.fetchone()

        if not row:
            return jsonify({"error": "Invalid or expired invite link"}), 400

        return jsonify({
            "personnel_id": str(row['personnel_id']),
            "full_name": row['full_name'],
            "email": row['email'],
        })
    finally:
        release_conn(conn)


@app.route('/api/freelancer/auth/set-password', methods=['POST', 'OPTIONS'])
def set_password():
    if request.method == 'OPTIONS':
        return '', 204

    data = request.get_json() or {}
    token = data.get('token', '')
    password = data.get('password', '')

    if not token or not password:
        return jsonify({"error": "Token and password required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Validate token
        cur.execute("""
            SELECT it.id, it.personnel_id, p.full_name, p.email, p.branch_id
            FROM forge_invite_tokens it
            JOIN personnel p ON p.id = it.personnel_id
            WHERE it.token = %s AND it.used = false AND it.expires_at > now()
        """, (token,))
        row = cur.fetchone()

        if not row:
            return jsonify({"error": "Invalid or expired invite link"}), 400

        # Hash password and update personnel
        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        cur.execute("""
            UPDATE personnel SET password_hash = %s, last_login_at = now()
            WHERE id = %s
        """, (hashed, row['personnel_id']))

        # Mark token as used
        cur.execute("UPDATE forge_invite_tokens SET used = true WHERE id = %s", (row['id'],))
        conn.commit()

        # Issue JWT
        jwt_token = jwt.encode({
            'personnel_id': str(row['personnel_id']),
            'email': row['email'],
            'branch_id': str(row['branch_id']) if row['branch_id'] else None,
            'role': 'freelancer',
            'exp': datetime.utcnow() + timedelta(days=JWT_EXPIRY_DAYS),
        }, JWT_SECRET, algorithm='HS256')

        return jsonify({
            "token": jwt_token,
            "user": {
                "id": str(row['personnel_id']),
                "name": row['full_name'],
                "email": row['email'],
            }
        })
    finally:
        release_conn(conn)


# ── TODAY ──

@app.route('/api/freelancer/me/today', methods=['GET', 'OPTIONS'])
@require_auth
def get_today():
    today = datetime.now().strftime('%Y-%m-%d')
    future = (datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d')
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Single query: all roster entries from today to +14 days
        cur.execute("""
            SELECT id, shift_date, start_time, end_time, booking_status,
                   personnel_status, worked_in_dept, branch_id,
                   clock_in_at, clock_out_at, break_start_at, break_minutes
            FROM roster_entries
            WHERE personnel_id = %s
            AND shift_date >= %s AND shift_date <= %s
            AND booking_status IN ('booked', 'accepted', 'confirmed', 'offered')
            ORDER BY shift_date
        """, (g.personnel_id, today, future))
        rows = cur.fetchall()

        # Fetch pay info
        cur.execute("SELECT pay_per_hour, currency FROM personnel WHERE id = %s", (g.personnel_id,))
        pay_row = cur.fetchone()
        pay_per_hour = float(pay_row['pay_per_hour'] or 0) if pay_row else 0
        pay_currency = (pay_row['currency'] or 'GBP') if pay_row else 'GBP'

        # Split results in Python
        today_shift = None
        upcoming = []
        offers_count = 0

        for r in rows:
            d = str(r['shift_date'])
            if d == today and r['booking_status'] in ('booked', 'accepted', 'confirmed') and not today_shift:
                today_shift = r
            elif d > today:
                upcoming.append(r)
            if r['booking_status'] == 'offered' and d >= today:
                offers_count += 1

        def serialize(s):
            if not s:
                return None
            return {
                'id': str(s['id']),
                'date': str(s['shift_date']),
                'start': str(s['start_time'])[:5] if s['start_time'] else '09:00',
                'end': str(s['end_time'])[:5] if s['end_time'] else '17:00',
                'status': s['booking_status'],
                'dept': s.get('worked_in_dept') or 'Printing',
                'location': 'Studio A, London',
            }

        # Determine clock status
        clock_status = "idle"
        clock_in_at = None
        break_start_at = None
        if today_shift:
            if today_shift.get('clock_in_at') and not today_shift.get('clock_out_at'):
                clock_in_at = today_shift['clock_in_at'].isoformat() if today_shift['clock_in_at'] else None
                if today_shift.get('break_start_at'):
                    clock_status = "break"
                    break_start_at = today_shift['break_start_at'].isoformat()
                else:
                    clock_status = "clocked"
            elif today_shift.get('clock_out_at'):
                clock_status = "completed"

        return jsonify({
            "today": serialize(today_shift),
            "clock_status": clock_status,
            "clock_in_at": clock_in_at,
            "break_start_at": break_start_at,
            "upcoming": [serialize(s) for s in upcoming[:10]],
            "pending_offers": offers_count,
            "pay_per_hour": pay_per_hour,
            "currency": pay_currency,
        })
    finally:
        release_conn(conn)


# ── AVAILABILITY ──

@app.route('/api/freelancer/me/availability', methods=['GET', 'OPTIONS'])
@require_auth
def get_availability():
    weeks = int(request.args.get('weeks', 2))
    start = datetime.now().strftime('%Y-%m-%d')
    end = (datetime.now() + timedelta(weeks=weeks)).strftime('%Y-%m-%d')

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT date, status, reason FROM freelancer_availability
            WHERE personnel_id = %s AND date >= %s AND date < %s
            ORDER BY date
        """, (g.personnel_id, start, end))
        rows = cur.fetchall()
        return jsonify({"days": [{"date": str(r['date']), "available": r['status'] == 'available', "reason": r.get('reason')} for r in rows]})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/me/availability', methods=['POST'])
@require_auth
def set_availability():
    data = request.get_json()
    days = (data or {}).get('days', [])
    if not days:
        return jsonify({"error": "No days provided"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        dates = [d['date'] for d in days]
        min_date = min(dates)
        max_date = max(dates)

        for d in days:
            status = 'available' if d.get('available') else 'unavailable'
            reason = d.get('reason') or None
            cur.execute("""
                INSERT INTO freelancer_availability (personnel_id, date, status, window_start, window_end, submitted_at, reason)
                VALUES (%s, %s, %s, %s, %s, now(), %s)
                ON CONFLICT (personnel_id, date) DO UPDATE SET status = EXCLUDED.status, submitted_at = now(), reason = EXCLUDED.reason
            """, (g.personnel_id, d['date'], status, min_date, max_date, reason))

        conn.commit()
        return jsonify({"updated_count": len(days)})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        release_conn(conn)


# ── CLOCK IN/OUT ──

@app.route('/api/freelancer/clock/scan', methods=['POST', 'OPTIONS'])
@require_auth
def clock_scan():
    data = request.get_json()
    qr_raw = (data or {}).get('qr_data', '')

    # Dev bypass
    if qr_raw in ('DEV_CLOCK_TOGGLE', 'SIMULATE_CLOCK_IN', 'SIMULATE_CLOCK_OUT'):
        return _handle_clock_toggle(g.personnel_id, g.branch_id, 'Printing')

    # Parse QR JSON
    try:
        qr = _json.loads(qr_raw)
    except Exception:
        return jsonify({"error": "Invalid QR code format"}), 400

    qr_date = qr.get('date', '')
    qr_branch = qr.get('branch_id', '')
    qr_token = qr.get('token', '')
    qr_type = qr.get('type', 'clock')
    qr_dept = qr.get('department', 'Printing')
    today = datetime.now().strftime('%Y-%m-%d')

    if qr_date != today:
        return jsonify({"error": "QR code expired — this code is for " + qr_date}), 400

    # Validate token against daily_qr_tokens
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT token FROM daily_qr_tokens
            WHERE branch_id = %s AND token_date = %s
        """, (qr_branch, today))
        row = cur.fetchone()

        if not row or row['token'] != qr_token:
            return jsonify({"error": "Invalid QR code"}), 400

        if qr_type == 'overtime':
            return _handle_overtime_scan(g.personnel_id, qr_branch, qr_dept)

        return _handle_clock_toggle(g.personnel_id, qr_branch, qr_dept)
    finally:
        release_conn(conn)


def _handle_clock_toggle(personnel_id, branch_id, department):
    """Toggle clock in/out based on current state. Writes to all 3 layers."""
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT id, clock_in_at, clock_out_at, break_minutes, start_time, end_time
            FROM roster_entries
            WHERE personnel_id = %s AND shift_date = %s
            AND booking_status IN ('booked', 'accepted')
            LIMIT 1
        """, (personnel_id, today))
        entry = cur.fetchone()

        # ── CLOCK IN: no roster entry exists ──
        if not entry:
            cur.execute("""
                INSERT INTO roster_entries (personnel_id, shift_date, branch_id, booking_status,
                    personnel_status, worked_in_dept, clock_in_at, start_time, end_time)
                VALUES (%s, %s, %s, 'accepted', 'present', %s, %s, '09:00', '17:00')
                ON CONFLICT (personnel_id, shift_date) DO UPDATE
                SET clock_in_at = EXCLUDED.clock_in_at, personnel_status = 'present',
                    worked_in_dept = EXCLUDED.worked_in_dept, booking_status = 'accepted'
                RETURNING id
            """, (personnel_id, today, branch_id, department, now))
            roster_id = cur.fetchone()['id']

            cur.execute("""
                INSERT INTO clock_entries (personnel_id, roster_entry_id, shift_date,
                    clock_in, department, branch_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'active')
                RETURNING id
            """, (personnel_id, roster_id, today, now, department, branch_id))
            ce_id = cur.fetchone()['id']

            _log_event(cur, ce_id, roster_id, personnel_id, 'clock_in', department)
            conn.commit()
            return jsonify({"action": "clocked_in", "department": department,
                            "time": now.strftime('%H:%M'), "clock_in_at": now.isoformat()})

        # ── CLOCK IN: roster entry exists, not clocked in yet ──
        if not entry['clock_in_at']:
            cur.execute("""
                UPDATE roster_entries SET clock_in_at = %s, worked_in_dept = %s, personnel_status = 'present'
                WHERE id = %s
            """, (now, department, entry['id']))

            cur.execute("""
                INSERT INTO clock_entries (personnel_id, roster_entry_id, shift_date,
                    clock_in, department, branch_id, status)
                VALUES (%s, %s, %s, %s, %s, %s, 'active')
                RETURNING id
            """, (personnel_id, entry['id'], today, now, department, branch_id))
            ce_id = cur.fetchone()['id']

            _log_event(cur, ce_id, entry['id'], personnel_id, 'clock_in', department)
            conn.commit()
            return jsonify({"action": "clocked_in", "department": department,
                            "time": now.strftime('%H:%M'), "clock_in_at": now.isoformat()})

        if entry['clock_out_at']:
            return jsonify({"error": "Already clocked out today"}), 400

        # ── CLOCK OUT: calculate hours ──
        clock_in = entry['clock_in_at']
        breaks = entry['break_minutes'] or 0
        elapsed_seconds = (now - clock_in).total_seconds() - (breaks * 60)
        hours_worked = elapsed_seconds / 3600

        if hours_worked < 8:
            scheduled_end = str(entry['end_time'])[:5] if entry['end_time'] else '17:00'
            return jsonify({"action": "early_checkout", "hours_worked": round(hours_worked, 2),
                            "scheduled_end": scheduled_end, "clock_in_at": clock_in.isoformat()})
        else:
            cur.execute("UPDATE roster_entries SET clock_out_at = %s WHERE id = %s", (now, entry['id']))
            ce_id = _get_active_clock_entry(cur, personnel_id, today)
            if ce_id:
                cur.execute("""
                    UPDATE clock_entries SET clock_out = %s,
                        worked_hours = %s, break_minutes = %s, status = 'pending_review'
                    WHERE id = %s
                """, (now, round(hours_worked, 2), breaks, ce_id))
                _log_event(cur, ce_id, entry['id'], personnel_id, 'clock_out',
                           metadata={"worked_hours": round(hours_worked, 2), "break_minutes": breaks})
            conn.commit()
            return jsonify({"action": "clocked_out", "hours_worked": round(hours_worked, 2),
                            "time": now.strftime('%H:%M')})
    finally:
        release_conn(conn)


def _handle_overtime_scan(personnel_id, branch_id, department):
    """Handle overtime department QR scan."""
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, clock_in_at, clock_out_at, break_minutes
            FROM roster_entries
            WHERE personnel_id = %s AND shift_date = %s
            AND booking_status IN ('booked', 'accepted')
            AND clock_in_at IS NOT NULL AND clock_out_at IS NULL
            LIMIT 1
        """, (personnel_id, today))
        entry = cur.fetchone()

        if not entry:
            return jsonify({"error": "Not clocked in"}), 400

        breaks = entry['break_minutes'] or 0
        hours_worked = ((now - entry['clock_in_at']).total_seconds() - breaks * 60) / 3600

        if hours_worked < 9:
            return jsonify({"error": "Not in overtime. {:.1f}h worked.".format(hours_worked)}), 400

        cur.execute("UPDATE roster_entries SET last_overtime_scan = %s, worked_in_dept = %s WHERE id = %s",
                    (now, department, entry['id']))

        ce_id = _get_active_clock_entry(cur, personnel_id, today)
        if ce_id:
            _log_event(cur, ce_id, entry['id'], personnel_id, 'overtime_scan',
                       department, {"hours_worked": round(hours_worked, 2)})

        conn.commit()
        next_scan = now + timedelta(minutes=30)
        return jsonify({"action": "overtime_confirmed", "department": department,
                        "hours_worked": round(hours_worked, 2), "next_scan_by": next_scan.strftime('%H:%M')})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/clock/force-out', methods=['POST', 'OPTIONS'])
@require_auth
def clock_force_out():
    """Force clock out (early checkout confirmed by user)."""
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, clock_in_at, break_minutes FROM roster_entries
            WHERE personnel_id = %s AND shift_date = %s
            AND clock_in_at IS NOT NULL AND clock_out_at IS NULL
            LIMIT 1
        """, (g.personnel_id, today))
        entry = cur.fetchone()

        if not entry:
            return jsonify({"error": "No active clock-in found"}), 400

        breaks = entry['break_minutes'] or 0
        hours_worked = round(((now - entry['clock_in_at']).total_seconds() - breaks * 60) / 3600, 2)

        cur.execute("UPDATE roster_entries SET clock_out_at = %s WHERE id = %s", (now, entry['id']))

        ce_id = _get_active_clock_entry(cur, g.personnel_id, today)
        if ce_id:
            cur.execute("""
                UPDATE clock_entries SET clock_out = %s,
                    worked_hours = %s, break_minutes = %s, status = 'pending_review'
                WHERE id = %s
            """, (now, hours_worked, breaks, ce_id))
            _log_event(cur, ce_id, entry['id'], g.personnel_id, 'force_out',
                       metadata={"worked_hours": hours_worked, "break_minutes": breaks, "reason": "early_checkout"})

        conn.commit()
        return jsonify({"action": "clocked_out", "time": now.strftime('%H:%M')})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/clock/break/start', methods=['POST', 'OPTIONS'])
@require_auth
def break_start():
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id FROM roster_entries
            WHERE personnel_id = %s AND shift_date = %s
            AND clock_in_at IS NOT NULL AND clock_out_at IS NULL LIMIT 1
        """, (g.personnel_id, today))
        entry = cur.fetchone()

        if not entry:
            return jsonify({"error": "Not clocked in"}), 400

        cur.execute("UPDATE roster_entries SET break_start_at = %s WHERE id = %s", (now, entry['id']))

        ce_id = _get_active_clock_entry(cur, g.personnel_id, today)
        break_number = 1
        if ce_id:
            cur.execute("SELECT count(*) as cnt FROM clock_events WHERE clock_entry_id = %s AND event_type = 'break_start'", (ce_id,))
            break_number = (cur.fetchone()['cnt'] or 0) + 1
            _log_event(cur, ce_id, entry['id'], g.personnel_id, 'break_start', metadata={"break_number": break_number})

        conn.commit()
        return jsonify({"break_started": now.strftime('%H:%M'), "break_number": break_number})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/clock/break/end', methods=['POST', 'OPTIONS'])
@require_auth
def break_end():
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, break_start_at, break_minutes FROM roster_entries
            WHERE personnel_id = %s AND shift_date = %s AND clock_in_at IS NOT NULL AND clock_out_at IS NULL LIMIT 1
        """, (g.personnel_id, today))
        entry = cur.fetchone()

        if not entry or not entry['break_start_at']:
            return jsonify({"error": "No active break"}), 400

        break_duration = int((now - entry['break_start_at']).total_seconds() / 60)
        total_breaks = (entry['break_minutes'] or 0) + break_duration

        cur.execute("UPDATE roster_entries SET break_start_at = NULL, break_minutes = %s WHERE id = %s",
                    (total_breaks, entry['id']))

        ce_id = _get_active_clock_entry(cur, g.personnel_id, today)
        break_number = 1
        if ce_id:
            cur.execute("SELECT count(*) as cnt FROM clock_events WHERE clock_entry_id = %s AND event_type = 'break_end'", (ce_id,))
            break_number = (cur.fetchone()['cnt'] or 0) + 1
            _log_event(cur, ce_id, entry['id'], g.personnel_id, 'break_end',
                       metadata={"duration_minutes": break_duration, "break_number": break_number, "total_break_minutes": total_breaks})

        conn.commit()
        return jsonify({"break_ended": now.strftime('%H:%M'), "break_minutes": break_duration, "total_break_minutes": total_breaks})
    finally:
        release_conn(conn)


# ── REQUEST SHIFTS ──

@app.route('/api/freelancer/me/request-shifts', methods=['POST', 'OPTIONS'])
@require_auth
def request_shifts():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT full_name FROM personnel WHERE id = %s", (g.personnel_id,))
        row = cur.fetchone()
        name = row['full_name'] if row else 'A freelancer'

        cur.execute("""
            INSERT INTO staff_alerts (alert_type, personnel_id, branch_id, message, status, created_at)
            VALUES ('shift_request', %s, %s, %s, 'unread', now())
        """, (g.personnel_id, g.branch_id, f'{name} is requesting more shifts'))
        conn.commit()
        return jsonify({"success": True})
    except Exception:
        conn.rollback()
        return jsonify({"success": True})
    finally:
        release_conn(conn)


# ── NOTIFICATIONS ──

@app.route('/api/freelancer/me/notifications', methods=['GET', 'OPTIONS'])
@require_auth
def get_notifications():
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, type, title, body, deep_link, read, created_at
            FROM forge_notifications
            WHERE personnel_id = %s AND created_at >= now() - interval '7 days'
            ORDER BY created_at DESC LIMIT 30
        """, (g.personnel_id,))
        rows = cur.fetchall()
        return jsonify({"notifications": [
            {
                "id": str(r['id']),
                "type": r['type'],
                "read": r['read'],
                "title": r['title'],
                "body": r['body'],
                "deep_link": r['deep_link'],
                "created_at": r['created_at'].isoformat() if r['created_at'] else None,
            } for r in rows
        ]})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/me/notifications/<notif_id>/read', methods=['POST', 'OPTIONS'])
@require_auth
def mark_notification_read(notif_id):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE forge_notifications SET read = true
            WHERE id = %s AND personnel_id = %s
        """, (notif_id, g.personnel_id))
        conn.commit()
        return jsonify({"ok": True})
    finally:
        release_conn(conn)


# ── FAILURE LOG (no auth — fire and forget from client) ──

@app.route('/api/freelancer/log/failure', methods=['POST', 'OPTIONS'])
def log_failure():
    if request.method == 'OPTIONS':
        return '', 204

    data = request.get_json() or {}
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO api_failure_logs (personnel_id, endpoint, payload, retries, first_attempted)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            data.get('user_id'),
            data.get('endpoint'),
            _json.dumps(data.get('body', {})),
            data.get('retries', 0),
            data.get('first_attempted'),
        ))
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        release_conn(conn)
    return jsonify({"ok": True})


# ── FCM TOKEN + PUSH ──

@app.route('/api/freelancer/me/fcm-token', methods=['POST', 'OPTIONS'])
@require_auth
def save_fcm_token():
    data = request.get_json() or {}
    token = data.get('token', '')
    if not token:
        return jsonify({"error": "Token required"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE personnel SET fcm_token = %s, fcm_token_updated_at = now()
            WHERE id = %s
        """, (token, g.personnel_id))
        conn.commit()
        return jsonify({"success": True})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/admin/push', methods=['POST', 'OPTIONS'])
def admin_push():
    if request.method == 'OPTIONS':
        return '', 204

    data = request.get_json() or {}
    personnel_id = data.get('personnel_id')
    title = data.get('title', 'Forge')
    body = data.get('body', '')
    link = data.get('link', '/')

    if not personnel_id:
        return jsonify({"error": "personnel_id required"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT fcm_token FROM personnel WHERE id = %s", (personnel_id,))
        row = cur.fetchone()
        if row and row['fcm_token']:
            send_push(row['fcm_token'], title, body, link)
        return jsonify({"success": True})
    finally:
        release_conn(conn)


# ── CLOCK EDIT REQUEST ──

@app.route('/api/freelancer/me/earnings/<entry_id>/edit-request', methods=['POST', 'OPTIONS'])
@require_auth
def request_clock_edit(entry_id):
    data = request.get_json() or {}
    edit_in = data.get('edit_clock_in', '')
    edit_out = data.get('edit_clock_out', '')
    edit_break = data.get('edit_break_minutes')
    notes = data.get('notes', '')

    if not notes:
        return jsonify({"error": "Reason is required"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Validate ownership + not already requested
        cur.execute("""
            SELECT id, edit_requested FROM clock_entries
            WHERE id = %s AND personnel_id = %s
        """, (entry_id, g.personnel_id))
        row = cur.fetchone()

        if not row:
            return jsonify({"error": "Entry not found"}), 404
        if row['edit_requested']:
            return jsonify({"error": "Edit already requested"}), 400

        cur.execute("""
            UPDATE clock_entries SET
                edit_requested = true,
                edit_clock_in = %s,
                edit_clock_out = %s,
                edit_break_minutes = %s,
                notes = %s,
                updated_at = now()
            WHERE id = %s
        """, (edit_in or None, edit_out or None, edit_break, notes, entry_id))
        conn.commit()

        return jsonify({"success": True})
    finally:
        release_conn(conn)


# ── EARNINGS ──

@app.route('/api/freelancer/me/earnings', methods=['GET', 'OPTIONS'])
@require_auth
def get_earnings():
    conn = get_conn()
    try:
        cur = conn.cursor()

        # Get pay info
        cur.execute("SELECT pay_per_hour, currency FROM personnel WHERE id = %s", (g.personnel_id,))
        pay_row = cur.fetchone()
        rate = float(pay_row['pay_per_hour'] or 0) if pay_row else 0
        currency = (pay_row['currency'] or 'GBP') if pay_row else 'GBP'

        # Get clock entries
        cur.execute("""
            SELECT id, shift_date, clock_in, clock_out, break_minutes,
                   worked_hours, overtime_hours, department, status, edit_requested
            FROM clock_entries
            WHERE personnel_id = %s
            ORDER BY shift_date DESC LIMIT 50
        """, (g.personnel_id,))
        rows = cur.fetchall()

        total_earned = 0
        pending = 0
        entries = []

        for r in rows:
            wh = float(r['worked_hours'] or 0)
            ot = float(r['overtime_hours'] or 0)
            gross = round(wh * rate, 2)
            ot_pay = round(ot * rate * 1.5, 2)
            total = round(gross + ot_pay, 2)

            if r['status'] == 'approved' or r['status'] == 'paid':
                total_earned += total
            elif r['status'] == 'pending_review':
                pending += total

            entries.append({
                'id': str(r['id']),
                'shift_date': str(r['shift_date']),
                'clock_in': r['clock_in'].strftime('%H:%M') if r['clock_in'] else None,
                'clock_out': r['clock_out'].strftime('%H:%M') if r['clock_out'] else None,
                'break_minutes': r['break_minutes'] or 0,
                'worked_hours': wh,
                'overtime_hours': ot,
                'department': r['department'],
                'status': r['status'],
                'pay_per_hour': rate,
                'currency': currency,
                'gross_pay': gross,
                'overtime_pay': ot_pay,
                'total_pay': total,
                'edit_requested': r['edit_requested'] or False,
            })

        return jsonify({
            'total_earned': round(total_earned, 2),
            'pending': round(pending, 2),
            'currency': currency,
            'entries': entries,
        })
    finally:
        release_conn(conn)


# ── OFFERS ──

@app.route('/api/freelancer/me/offers', methods=['GET', 'OPTIONS'])
@require_auth
def get_offers():
    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT re.id, re.shift_date, re.start_time, re.end_time, re.booking_status,
                   re.worked_in_dept, p.pay_per_hour, p.currency
            FROM roster_entries re
            JOIN personnel p ON p.id = re.personnel_id
            WHERE re.personnel_id = %s AND re.booking_status IN ('offered', 'accepted', 'declined')
            AND re.shift_date >= %s
            ORDER BY re.shift_date
        """, (g.personnel_id, today))
        rows = cur.fetchall()

        offers = []
        for r in rows:
            hours = 8
            if r['start_time'] and r['end_time']:
                s = r['start_time']
                e = r['end_time']
                hours = (e.hour * 60 + e.minute - s.hour * 60 - s.minute) / 60
            rate = float(r['pay_per_hour'] or 12)
            offers.append({
                'id': str(r['id']),
                'date': str(r['shift_date']),
                'start': str(r['start_time'])[:5] if r['start_time'] else '09:00',
                'end': str(r['end_time'])[:5] if r['end_time'] else '17:00',
                'status': r['booking_status'],
                'dept': r['worked_in_dept'] or 'Printing',
                'location': 'Studio A',
                'earnings': round(hours * rate, 2),
                'currency': r['currency'] or 'GBP',
            })

        return jsonify({"offers": offers})
    finally:
        release_conn(conn)


@app.route('/api/freelancer/me/offers/respond', methods=['POST', 'OPTIONS'])
@require_auth
def respond_to_offers():
    data = request.get_json()
    responses = (data or {}).get('responses', [])
    if not responses:
        return jsonify({"error": "No responses provided"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        accepted = 0
        declined = 0
        for r in responses:
            new_status = 'confirmed' if r.get('accepted', False) else 'declined'
            cur.execute("""
                UPDATE roster_entries SET booking_status = %s, updated_at = now()
                WHERE id = %s AND personnel_id = %s AND booking_status = 'offered'
            """, (new_status, r.get('shift_id'), g.personnel_id))
            if cur.rowcount > 0:
                if r.get('accepted'):
                    accepted += 1
                else:
                    declined += 1
        conn.commit()
        return jsonify({"accepted": accepted, "declined": declined, "total": accepted + declined})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        release_conn(conn)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5050)), debug=False)
