# app.py
from __future__ import annotations
import os, json, hashlib, hmac
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

USERS_FILE = os.path.join(DATA_DIR, "users.txt")        # JSONL
NEWS_FILE  = os.path.join(DATA_DIR, "newsletter.txt")   # JSONL
SECRET = os.environ.get("GTP_SECRET","gtp-demo-secret")

app = Flask(__name__, static_folder=None)
CORS(app, resources={r"/api/*":{"origins":"*"}})

def _hash_pw(pw: str) -> str:
    # hash estable con HMAC-SHA256 (demo).
    return hmac.new(SECRET.encode(), pw.encode(), hashlib.sha256).hexdigest()

def _append_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False)+"\n")

def _find_user(email: str):
    if not os.path.exists(USERS_FILE): return None
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if row.get("email","").lower()==email.lower():
                return row
    return None

@app.post("/api/register")
def api_register():
    data = request.get_json(force=True)
    req = ["name","last","email","password","birth"]
    if not all(data.get(k) for k in req):
        return jsonify({"message":"Campos incompletos"}), 400
    email = data["email"].strip().lower()
    if _find_user(email):
        return jsonify({"message":"Ya existe un usuario con ese email"}), 409
    user = {
        "name": data["name"].strip(),
        "last": data["last"].strip(),
        "email": email,
        "pass_hash": _hash_pw(data["password"]),
        "birth": data["birth"],
        "dni": data.get("dni","").strip(),
        "created_at": datetime.utcnow().isoformat()+"Z"
    }
    _append_jsonl(USERS_FILE, user)
    return jsonify({"ok":True, "email": email})

@app.post("/api/login")
def api_login():
    data = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()
    pw    = (data.get("password") or "")
    u = _find_user(email)
    if not u:
        return jsonify({"message":"Usuario no encontrado"}), 404
    if _hash_pw(pw) != u.get("pass_hash"):
        return jsonify({"message":"Contraseña incorrecta"}), 401
    # token demo: email + timestamp hash (no JWT)
    token = hmac.new(SECRET.encode(), (email+u["created_at"]).encode(), hashlib.sha256).hexdigest()
    return jsonify({"ok":True,"email":email,"token":token})

@app.post("/api/newsletter")
def api_news():
    data = request.get_json(force=True)
    email = (data.get("email") or "").strip().lower()
    topic = (data.get("topic") or "General").strip()
    if not email:
        return jsonify({"message":"Email requerido"}), 400
    rec = {"email":email,"topic":topic,"ts":datetime.utcnow().isoformat()+"Z"}
    _append_jsonl(NEWS_FILE, rec)
    return jsonify({"ok":True})

# Servir index y estáticos si lo ejecutas en la misma carpeta
@app.get("/")
def root():
    return send_from_directory(APP_DIR, "index.html")

@app.get("/<path:path>")
def any_static(path):
    return send_from_directory(APP_DIR, path)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
