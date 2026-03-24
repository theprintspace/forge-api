"""
Forge API — Standalone Flask service for the Forge freelancer mobile app.
Connects to production Supabase. Deployed on Railway.
"""

import os
import jwt
import bcrypt
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, g
from flask_cors import CORS

app = Flask(__name__)
CORS(app, origins=[
    "https://forge-app-sigma.vercel.app",
    "https://forge.bigops.in",
    "http://localhost:3000",
    "http://localhost:5173",
], supports_credentials=True, allow_headers=["Authorization", "Content-Type"])

JWT_SECRET = os.environ.get('FREELANCER_JWT_SECRET', 'forge-dev-secret-change-me')
JWT_EXPIRY_HOURS = 24


# ── DB Connection ──

def get_conn():
    return psycopg2.connect(
        host=os.environ.get("SUPABASE_DB_HOST", "db.vxhyfjqpmjsxvhyuxaar.supabase.co"),
        port=int(os.environ.get("SUPABASE_DB_PORT", "5432")),
        dbname="postgres",
        user=os.environ.get("SUPABASE_DB_USER", "postgres"),
        password=os.environ.get("SUPABASE_DB_PASSWORD", ""),
        options="-c search_path=public",
        cursor_factory=psycopg2.extras.RealDictCursor
    )


# ── Auth Middleware ──

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
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


# ── Health Check ──

@app.route('/', methods=['GET'])
def health():
    return jsonify({"service": "forge-api", "status": "ok", "time": datetime.utcnow().isoformat()})


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
            "SELECT id, full_name, email, branch_id, password_hash FROM personnel "
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

        token = jwt.encode({
            'personnel_id': str(user['id']),
            'email': user['email'],
            'branch_id': str(user['branch_id']) if user['branch_id'] else None,
            'role': 'freelancer',
            'exp': datetime.utcnow() + timedelta(hours=JWT_EXPIRY_HOURS),
        }, JWT_SECRET, algorithm='HS256')

        return jsonify({
            "token": token,
            "user": {
                "id": str(user['id']),
                "name": user['full_name'],
                "email": user['email'],
            }
        })
    finally:
        conn.close()


@app.route('/api/freelancer/auth/reset-password', methods=['POST', 'OPTIONS'])
def reset_password():
    if request.method == 'OPTIONS':
        return '', 204
    return jsonify({"message": "If that email exists, a reset link has been sent."})


# ── TODAY ──

@app.route('/api/freelancer/me/today', methods=['GET', 'OPTIONS'])
@require_auth
def get_today():
    if request.method == 'OPTIONS':
        return '', 204

    today = datetime.now().strftime('%Y-%m-%d')
    conn = get_conn()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT re.id, re.shift_date, re.start_time, re.end_time, re.booking_status,
                   re.personnel_status, re.worked_in_dept, re.branch_id,
                   re.clock_in_at, re.clock_out_at, re.break_start_at, re.break_minutes
            FROM roster_entries re
            WHERE re.personnel_id = %s AND re.shift_date = %s
            AND re.booking_status IN ('booked', 'accepted')
            LIMIT 1
        """, (g.personnel_id, today))
        today_shift = cur.fetchone()

        cur.execute("""
            SELECT re.id, re.shift_date, re.start_time, re.end_time, re.booking_status,
                   re.worked_in_dept
            FROM roster_entries re
            WHERE re.personnel_id = %s AND re.shift_date > %s AND re.shift_date <= %s
            AND re.booking_status IN ('booked', 'accepted', 'offered')
            ORDER BY re.shift_date LIMIT 10
        """, (g.personnel_id, today, (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')))
        upcoming = cur.fetchall()

        cur.execute("""
            SELECT count(*) as cnt FROM roster_entries
            WHERE personnel_id = %s AND booking_status = 'offered' AND shift_date >= %s
        """, (g.personnel_id, today))
        offers_count = cur.fetchone()['cnt']

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
            "upcoming": [serialize(s) for s in upcoming],
            "pending_offers": offers_count,
        })
    finally:
        conn.close()


# ── AVAILABILITY ──

@app.route('/api/freelancer/me/availability', methods=['GET', 'OPTIONS'])
@require_auth
def get_availability():
    if request.method == 'OPTIONS':
        return '', 204

    weeks = int(request.args.get('weeks', 2))
    start = datetime.now().strftime('%Y-%m-%d')
    end = (datetime.now() + timedelta(weeks=weeks)).strftime('%Y-%m-%d')

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT date, status FROM freelancer_availability
            WHERE personnel_id = %s AND date >= %s AND date < %s
            ORDER BY date
        """, (g.personnel_id, start, end))
        rows = cur.fetchall()
        return jsonify({"days": [{"date": str(r['date']), "available": r['status'] == 'available'} for r in rows]})
    finally:
        conn.close()


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
            cur.execute("""
                INSERT INTO freelancer_availability (personnel_id, date, status, window_start, window_end, available)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (personnel_id, date) DO UPDATE SET status = EXCLUDED.status, available = EXCLUDED.available
            """, (g.personnel_id, d['date'], status, min_date, max_date, d.get('available', True)))

        conn.commit()
        return jsonify({"updated_count": len(days)})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


# ── CLOCK IN/OUT ──

@app.route('/api/freelancer/clock/scan', methods=['POST', 'OPTIONS'])
@require_auth
def clock_scan():
    if request.method == 'OPTIONS':
        return '', 204

    data = request.get_json()
    qr_raw = (data or {}).get('qr_data', '')

    # Dev bypass
    if qr_raw in ('DEV_CLOCK_TOGGLE', 'SIMULATE_CLOCK_IN', 'SIMULATE_CLOCK_OUT'):
        return _handle_clock_toggle(g.personnel_id, g.branch_id, 'Printing')

    # Parse QR JSON
    import json as _json
    try:
        qr = _json.loads(qr_raw)
    except Exception:
        return jsonify({"error": "Invalid QR code format"}), 400

    qr_date = qr.get('date', '')
    qr_branch = qr.get('branch_id', '')
    qr_token = qr.get('token', '')
    qr_type = qr.get('type', 'clock')  # backwards compat: no type = clock
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
        conn.close()


def _handle_clock_toggle(personnel_id, branch_id, department):
    """Toggle clock in/out based on current state."""
    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()

        # Find today's roster entry
        cur.execute("""
            SELECT id, clock_in_at, clock_out_at, break_minutes, start_time, end_time
            FROM roster_entries
            WHERE personnel_id = %s AND shift_date = %s
            AND booking_status IN ('booked', 'accepted')
            LIMIT 1
        """, (personnel_id, today))
        entry = cur.fetchone()

        if not entry:
            # No existing entry — create one and clock in
            cur.execute("""
                INSERT INTO roster_entries (personnel_id, shift_date, branch_id, booking_status,
                    personnel_status, worked_in_dept, clock_in_at, start_time, end_time)
                VALUES (%s, %s, %s, 'accepted', 'present', %s, %s, '09:00', '17:00')
                ON CONFLICT (personnel_id, shift_date) DO UPDATE
                SET clock_in_at = EXCLUDED.clock_in_at, personnel_status = 'present',
                    worked_in_dept = EXCLUDED.worked_in_dept, booking_status = 'accepted'
                RETURNING id
            """, (personnel_id, today, branch_id, department, now))
            conn.commit()
            return jsonify({
                "action": "clocked_in",
                "department": department,
                "time": now.strftime('%H:%M'),
                "clock_in_at": now.isoformat(),
            })

        if not entry['clock_in_at']:
            # Not clocked in yet — clock in
            cur.execute("""
                UPDATE roster_entries SET clock_in_at = %s, worked_in_dept = %s, personnel_status = 'present'
                WHERE id = %s
            """, (now, department, entry['id']))
            conn.commit()
            return jsonify({
                "action": "clocked_in",
                "department": department,
                "time": now.strftime('%H:%M'),
                "clock_in_at": now.isoformat(),
            })

        if entry['clock_out_at']:
            # Already clocked out today
            return jsonify({"error": "Already clocked out today"}), 400

        # Clocked in — calculate hours and decide action
        clock_in = entry['clock_in_at']
        breaks = entry['break_minutes'] or 0
        elapsed_seconds = (now - clock_in).total_seconds() - (breaks * 60)
        hours_worked = elapsed_seconds / 3600

        if hours_worked < 8:
            # Early checkout warning
            scheduled_end = str(entry['end_time'])[:5] if entry['end_time'] else '17:00'
            return jsonify({
                "action": "early_checkout",
                "hours_worked": round(hours_worked, 2),
                "scheduled_end": scheduled_end,
                "clock_in_at": clock_in.isoformat(),
            })
        else:
            # Normal clock out
            cur.execute("""
                UPDATE roster_entries SET clock_out_at = %s WHERE id = %s
            """, (now, entry['id']))
            conn.commit()
            return jsonify({
                "action": "clocked_out",
                "hours_worked": round(hours_worked, 2),
                "time": now.strftime('%H:%M'),
            })
    finally:
        conn.close()


def _handle_overtime_scan(personnel_id, branch_id, department):
    """Handle overtime department QR scan — freelancer must be clocked in and past 9 hours."""
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

        clock_in = entry['clock_in_at']
        breaks = entry['break_minutes'] or 0
        elapsed_seconds = (now - clock_in).total_seconds() - (breaks * 60)
        hours_worked = elapsed_seconds / 3600

        if hours_worked < 9:
            return jsonify({"error": "Not in overtime. You have worked {:.1f} hours — overtime starts at 9 hours.".format(hours_worked)}), 400

        # Record the overtime scan
        cur.execute("""
            UPDATE roster_entries SET last_overtime_scan = %s, worked_in_dept = %s
            WHERE id = %s
        """, (now, department, entry['id']))
        conn.commit()

        next_scan_time = now + timedelta(minutes=30)
        return jsonify({
            "action": "overtime_confirmed",
            "department": department,
            "hours_worked": round(hours_worked, 2),
            "next_scan_by": next_scan_time.strftime('%H:%M'),
        })
    finally:
        conn.close()


@app.route('/api/freelancer/clock/force-out', methods=['POST', 'OPTIONS'])
@require_auth
def clock_force_out():
    """Force clock out (used when user confirms early checkout)."""
    if request.method == 'OPTIONS':
        return '', 204

    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE roster_entries SET clock_out_at = %s
            WHERE personnel_id = %s AND shift_date = %s AND clock_in_at IS NOT NULL AND clock_out_at IS NULL
        """, (now, g.personnel_id, today))
        conn.commit()

        if cur.rowcount == 0:
            return jsonify({"error": "No active clock-in found"}), 400

        return jsonify({"action": "clocked_out", "time": now.strftime('%H:%M')})
    finally:
        conn.close()


@app.route('/api/freelancer/clock/break/start', methods=['POST', 'OPTIONS'])
@require_auth
def break_start():
    if request.method == 'OPTIONS':
        return '', 204

    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE roster_entries SET break_start_at = %s
            WHERE personnel_id = %s AND shift_date = %s AND clock_in_at IS NOT NULL AND clock_out_at IS NULL
        """, (now, g.personnel_id, today))
        conn.commit()

        if cur.rowcount == 0:
            return jsonify({"error": "Not clocked in"}), 400

        return jsonify({"break_started": now.strftime('%H:%M')})
    finally:
        conn.close()


@app.route('/api/freelancer/clock/break/end', methods=['POST', 'OPTIONS'])
@require_auth
def break_end():
    if request.method == 'OPTIONS':
        return '', 204

    today = datetime.now().strftime('%Y-%m-%d')
    now = datetime.utcnow()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, break_start_at, break_minutes FROM roster_entries
            WHERE personnel_id = %s AND shift_date = %s AND clock_in_at IS NOT NULL AND clock_out_at IS NULL
            LIMIT 1
        """, (g.personnel_id, today))
        entry = cur.fetchone()

        if not entry or not entry['break_start_at']:
            return jsonify({"error": "No active break"}), 400

        break_duration = int((now - entry['break_start_at']).total_seconds() / 60)
        total_breaks = (entry['break_minutes'] or 0) + break_duration

        cur.execute("""
            UPDATE roster_entries SET break_start_at = NULL, break_minutes = %s
            WHERE id = %s
        """, (total_breaks, entry['id']))
        conn.commit()

        return jsonify({
            "break_ended": now.strftime('%H:%M'),
            "break_minutes": break_duration,
            "total_break_minutes": total_breaks,
        })
    finally:
        conn.close()


# ── OFFERS ──

@app.route('/api/freelancer/me/offers', methods=['GET', 'OPTIONS'])
@require_auth
def get_offers():
    if request.method == 'OPTIONS':
        return '', 204

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
        conn.close()


@app.route('/api/freelancer/me/offers/respond', methods=['POST', 'OPTIONS'])
@require_auth
def respond_to_offers():
    if request.method == 'OPTIONS':
        return '', 204

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
            new_status = 'accepted' if r.get('accepted', False) else 'declined'
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
        conn.close()


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5050)), debug=False)
