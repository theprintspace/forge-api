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
                   re.personnel_status, re.worked_in_dept, re.branch_id
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

        return jsonify({
            "today": serialize(today_shift),
            "clock_status": "idle",
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
