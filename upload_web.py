import os
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, jsonify, send_file, session
from werkzeug.utils import secure_filename
import pandas as pd
import requests
import uuid
import decimal
import time
import collections
import bcrypt
import base64
import io
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

DEST_DIR = r"C:\Users\lelee\OneDrive\Documentos\ALX\TRAE ALX\performance"
os.makedirs(DEST_DIR, exist_ok=True)

ALLOWED = {"xlsx", "csv"}

app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = os.getenv("APP_SECRET") or os.urandom(24)

LOGO_PATH = r"C:\Users\lelee\Downloads\logoalx.png"
if not os.path.exists(LOGO_PATH):
    LOGO_PATH = os.path.join(app.static_folder, "logo.png")

# carregar .env (opcional)
if load_dotenv:
    env_default = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_default):
        load_dotenv(env_default)
    secure_env = r"C:\secure\inter.env"
    if os.path.exists(secure_env):
        load_dotenv(secure_env)
    user_env = r"C:\env\.env.txt"
    if os.path.exists(user_env):
        load_dotenv(user_env)

CLIENT_ID = os.getenv("INTER_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("INTER_CLIENT_SECRET", "")
CERT_PATH = os.getenv("INTER_CERT_PATH", "")
KEY_PATH = os.getenv("INTER_KEY_PATH", "")
BASE_URL = os.getenv("INTER_BASE_URL", "").rstrip("/")
TOKEN_URL = os.getenv("INTER_TOKEN_URL", f"{BASE_URL}/oauth/token" if BASE_URL else "")
PIX_URL = os.getenv("INTER_PIX_URL", f"{BASE_URL}/pix/v1/payments" if BASE_URL else "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
BUCKET = "performance"
SERVERLESS = bool(os.getenv("VERCEL")) or (not os.access(DEST_DIR, os.W_OK))

@app.template_filter("datetime")
def fmt_datetime(ts):
    try:
        return datetime.fromtimestamp(float(ts)).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(ts)

@app.template_filter("filesize")
def fmt_filesize(size):
    try:
        size = float(size)
        if size < 1024:
            return f"{size:.0f} B"
        if size < 1024*1024:
            return f"{size/1024:.2f} KB"
        return f"{size/1024/1024:.2f} MB"
    except Exception:
        return str(size)

@app.get("/")
def index():
    if not session.get("user"):
        return redirect(url_for("login"))
    files = []
    if SERVERLESS:
        files = storage_list()
    else:
        for name in os.listdir(DEST_DIR):
            p = os.path.join(DEST_DIR, name)
            if os.path.isfile(p):
                files.append((name, os.path.getmtime(p), os.path.getsize(p)))
        files.sort(key=lambda x: x[1], reverse=True)
    return render_template("index.html", files=files, dest=DEST_DIR, logo_url=url_for("logo"), user=session.get("user"))

@app.post("/upload")
def upload():
    if not session.get("user"):
        return redirect(url_for("login"))
    f = request.files.get("file")
    if not f or f.filename == "":
        return jsonify({"ok": False, "error": "arquivo ausente"}), 400
    fn = secure_filename(f.filename)
    ext = fn.rsplit(".", 1)[-1].lower() if "." in fn else ""
    if ext not in ALLOWED:
        return jsonify({"ok": False, "error": "extensão inválida"}), 400
    base = os.path.splitext(fn)[0]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_name = f"{base}_{ts}.{ext}"
    if SERVERLESS:
        content_type = "text/csv" if ext == "csv" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        storage_upload(final_name, f.read(), content_type)
    else:
        path = os.path.join(DEST_DIR, final_name)
        f.save(path)
    return redirect(url_for("index"))

@app.get("/download/<name>")
def download(name):
    if not session.get("user"):
        return redirect(url_for("login"))
    if SERVERLESS:
        data = storage_download(name)
        return send_file(io.BytesIO(data), download_name=name, as_attachment=True)
    return send_from_directory(DEST_DIR, name, as_attachment=True)

@app.get("/logo")
def logo():
    return send_file(LOGO_PATH)

@app.get("/favicon.ico")
def favicon():
    return send_file(LOGO_PATH)

def latest_file():
    files = [f for f in os.listdir(DEST_DIR) if os.path.isfile(os.path.join(DEST_DIR, f))]
    files = [f for f in files if f.lower().endswith((".xlsx", ".csv"))]
    if not files:
        return None
    files.sort(key=lambda n: os.path.getmtime(os.path.join(DEST_DIR, n)), reverse=True)
    return files[0]

def read_df(path):
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    return pd.read_excel(path)

def read_df_bytes(data, name):
    ext = (name.rsplit(".",1)[-1] if "." in name else "").lower()
    if ext == "csv":
        return pd.read_csv(io.BytesIO(data))
    return pd.read_excel(io.BytesIO(data))

def storage_list():
    if not (SUPABASE_URL and SUPABASE_KEY):
        return []
    url = f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    body = {"prefix": "", "limit": 100, "offset": 0, "sortBy": {"column": "name", "order": "desc"}}
    r = requests.post(url, headers=headers, json=body, timeout=20)
    r.raise_for_status()
    items = r.json() or []
    out = []
    for it in items:
        name = it.get("name")
        size = (it.get("metadata") or {}).get("size") or it.get("size") or 0
        ts = it.get("updated_at") or it.get("created_at") or ""
        try:
            dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
            epoch = dt.timestamp()
        except Exception:
            epoch = time.time()
        out.append((name, epoch, size))
    return out

def storage_upload(name, content, content_type="application/octet-stream"):
    if not (SUPABASE_URL and SUPABASE_KEY):
        return False
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{name}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": content_type}
    r = requests.post(url, headers=headers, data=content, timeout=30)
    r.raise_for_status()
    return True

def storage_download(name):
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{name}"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.content

def get_cert_paths():
    cp = CERT_PATH
    kp = KEY_PATH
    ok = cp and os.path.exists(cp) and kp and os.path.exists(kp)
    if ok:
        return cp, kp
    cert_b64 = os.getenv("INTER_CERT_B64")
    key_b64 = os.getenv("INTER_KEY_B64")
    if cert_b64 and key_b64:
        tmpc = "/tmp/cert.pem"
        tmpk = "/tmp/key.pem"
        with open(tmpc, "wb") as f: f.write(base64.b64decode(cert_b64))
        with open(tmpk, "wb") as f: f.write(base64.b64decode(key_b64))
        return tmpc, tmpk
    return cp, kp
def norm_phone(s):
    s = "".join(ch for ch in str(s) if ch.isdigit())
    if s.startswith("0"):
        s = s[1:]
    if not s.startswith("55"):
        s = "55" + s
    return s

def guess_key_type(k):
    k = str(k).strip()
    if "@" in k and "." in k:
        return "email"
    only = "".join(ch for ch in k if ch.isdigit())
    if len(only) == 11:
        return "cpf"
    if len(only) == 14:
        return "cnpj"
    if len(only) >= 10 and len(only) <= 15:
        return "telefone"
    return "aleatoria"

def fmt_amount(v):
    s = str(v).replace(",", ".").strip()
    return decimal.Decimal(s)

def get_token():
    cp, kp = get_cert_paths()
    if not (CLIENT_ID and CLIENT_SECRET and cp and kp and TOKEN_URL):
        raise RuntimeError("Configuração do Banco Inter ausente")
    data = {"grant_type": "client_credentials"}
    r = requests.post(TOKEN_URL, data=data, auth=(CLIENT_ID, CLIENT_SECRET), cert=(cp, kp), timeout=30)
    r.raise_for_status()
    return r.json().get("access_token")

def pay_pix(token, row):
    if not PIX_URL:
        raise RuntimeError("INTER_PIX_URL/BASE_URL não configurado")
    chave = str(row["chave_pix"]).strip()
    tipo = guess_key_type(chave)
    if tipo == "telefone":
        chave = norm_phone(chave)
    valor = fmt_amount(row["valor"])
    descricao = str(row.get("descricao", "") or "")[:140]
    idem = str(row.get("id_pagamento") or uuid.uuid4())
    payload = {"valor": f"{valor:.2f}", "chave": chave, "descricao": descricao, "id": idem}
    headers = {"Authorization": f"Bearer {token}", "Idempotency-Key": idem, "Content-Type": "application/json"}
    cp, kp = get_cert_paths()
    r = requests.post(PIX_URL, json=payload, headers=headers, cert=(cp, kp), timeout=40)
    if r.status_code >= 400:
        return {"ok": False, "status": r.status_code, "error": r.text}
    return {"ok": True, "status": r.status_code, "data": r.json()}

def validate_df(df):
    req = ["id_pagamento", "nome", "documento", "chave_pix", "valor", "descricao"]
    cols = [c.lower().replace(" ", "_") for c in df.columns]
    miss = [c for c in req if c not in cols]
    if miss:
        raise RuntimeError("faltam colunas: " + ", ".join(miss))

@app.get("/process")
def process_get():
    if not session.get("user"):
        return redirect(url_for("login"))
    try:
        fname = latest_file()
        df = None
        if fname and not SERVERLESS:
            path = os.path.join(DEST_DIR, fname)
            df = read_df(path)
        else:
            items = storage_list()
            if not items:
                return jsonify({"ok": False, "error": "sem arquivo para processar"}), 400
            fname = items[0][0]
            data = storage_download(fname)
            df = read_df_bytes(data, fname)
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        validate_df(df)
        token = get_token()
        results = []
        key_counter = collections.Counter()
        for _, row in df.iterrows():
            try:
                res = pay_pix(token, row)
            except Exception as e:
                res = {"ok": False, "status": 0, "error": str(e)}
            res["id_pagamento"] = row.get("id_pagamento")
            res["valor"] = row.get("valor")
            res["chave_pix"] = row.get("chave_pix")
            try:
                val_dec = fmt_amount(row.get("valor"))
                res["value"] = float(val_dec)
            except Exception:
                res["value"] = 0.0
            kt = guess_key_type(row.get("chave_pix"))
            res["key_type"] = kt
            key_counter[kt] += 1
            results.append(res)
            time.sleep(0.2)
        okc = sum(1 for r in results if r["ok"])
        failc = len(results) - okc
        val_ok = sum(r["value"] for r in results if r["ok"])
        val_fail = sum(r["value"] for r in results if not r["ok"])
        key_labels = list(key_counter.keys())
        key_data = [key_counter[k] for k in key_labels]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_name = f"pix_exec_{ts}.csv"
        csv_text = pd.DataFrame(results).to_csv(index=False, encoding="utf-8")
        if SERVERLESS:
            storage_upload(log_name, csv_text.encode("utf-8"), "text/csv")
        else:
            with open(os.path.join(DEST_DIR, log_name), "w", encoding="utf-8") as f:
                f.write(csv_text)
        preview = results[:10]
        return render_template(
            "results.html",
            logo_url=url_for("logo"),
            total=len(results),
            ok=okc,
            fail=failc,
            value_ok=val_ok,
            value_fail=val_fail,
            key_labels=key_labels,
            key_data=key_data,
            log_name=log_name,
            file_name=fname,
            preview=preview
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

def supa_get_user(email):
    if not (SUPABASE_URL and SUPABASE_KEY):
        raise RuntimeError("Supabase não configurado")
    url = f"{SUPABASE_URL}/rest/v1/users_auth"
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    params = {"select": "email,password_hash,active", "email": f"eq.{email}", "limit": "1"}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception:
        params = {"select": "email,password_hash", "email": f"eq.{email}", "limit": "1"}
        r = requests.get(url, headers=headers, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
    return data[0] if data else None

@app.get("/login")
def login():
    return render_template("login.html", logo_url=url_for("logo"), error=request.args.get("error"))

@app.post("/login")
def login_post():
    email = (request.form.get("email") or "").strip().lower()
    password = request.form.get("password") or ""
    try:
        user = supa_get_user(email)
        if not user or (user.get("active") is False):
            return redirect(url_for("login", error="Credenciais inválidas"))
        stored = user.get("password_hash") or ""
        ok = False
        try:
            ok = bcrypt.checkpw(password.encode("utf-8"), stored.encode("utf-8"))
        except Exception:
            ok = stored == password
        if not ok:
            return redirect(url_for("login", error="Credenciais inválidas"))
        session["user"] = email
        return redirect(url_for("index"))
    except Exception:
        return redirect(url_for("login", error="Falha de autenticação"))

@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
