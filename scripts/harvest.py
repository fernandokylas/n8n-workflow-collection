import base64, hashlib, json, os, re, sys, time
from pathlib import Path
import requests, yaml

API = "https://api.github.com"
SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {os.getenv('GH_TOKEN','')}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
})

ROOT = Path(__file__).resolve().parents[1]
CATALOGUE = ROOT / "catalogue"
INGESTED = ROOT / "ingested"
INDEX = CATALOGUE / "index.json"
CATALOGUE.mkdir(parents=True, exist_ok=True)
(INGESTED / "by-source").mkdir(parents=True, exist_ok=True)
(INGESTED / "by-integration").mkdir(parents=True, exist_ok=True)

def backoff(a): time.sleep(min(60, (2 ** a) + (a * 0.1)))

def gh(url, params=None, raw=False, attempts=5):
    for i in range(attempts):
        r = SESSION.get(url, params=params, timeout=30)
        if r.status_code in (408, 429) or r.status_code >= 500:
            backoff(i); continue
        if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
            wait = int(r.headers.get("X-RateLimit-Reset","0")) - int(time.time())
            time.sleep(max(1, min(wait, 120))); continue
        r.raise_for_status()
        return r.content if raw else r.json()
    r.raise_for_status()

def canonical_json_bytes(obj):
    return json.dumps(obj, sort_keys=True, separators=(",",":")).encode("utf-8")

def sha256_bytes(b): return hashlib.sha256(b).hexdigest()

def detect_integrations(wf):
    names = set()
    for n in wf.get("nodes", []):
        t = n.get("type") or ""
        res = (n.get("parameters") or {}).get("resource") or ""
        for val in (t, res):
            m = re.search(r"(Airtable|Slack|Gmail|Telegram|Google[s ]?Drive|Notion|Discord|HTTP|Webhook)", val, re.I)
            if m: names.add(m.group(1).lower())
    return sorted(names) or ["uncategorised"]

def save_catalogue_entry(idx, entry):
    key = entry["hash"]
    if key in idx:
        idx[key]["sources"].append(entry["source"])
    else:
        idx[key] = entry

def iter_json_files(owner, repo, branch, include_globs):
    tree = gh(f"{API}/repos/{owner}/{repo}/git/trees/{branch}", params={"recursive":"1"})
    for item in tree.get("tree", []):
        if item.get("type") == "blob" and item.get("path","").endswith(".json"):
            from pathlib import Path as P
            if include_globs and not any(P(item["path"]).match(p) for p in include_globs):
                continue
            yield item["path"], item["sha"]

def fetch_json(owner, repo, path, ref):
    meta = gh(f"{API}/repos/{owner}/{repo}/contents/{path}", params={"ref": ref})
    import base64
    content = base64.b64decode(meta["content"])
    return json.loads(content)

def main():
    sources = yaml.safe_load((ROOT / "sources.yaml").read_text())["sources"]
    idx = {}
    if INDEX.exists():
        try: idx = json.loads(INDEX.read_text())
        except: idx = {}

    for s in sources:
        owner, repo, branch = s["owner"], s["repo"], s.get("branch","main")
        mode = s["mode"]
        include = s.get("include", [])
        for path, blob_sha in iter_json_files(owner, repo, branch, include):
            url = f"https://github.com/{owner}/{repo}/blob/{branch}/{path}"
            entry = {
                "source": {"owner": owner, "repo": repo, "path": path, "url": url, "blob_sha": blob_sha},
                "hash": "",
                "integrations": [],
                "stored_at": None,
                "licence": "unknown"
            }
            if mode == "link-only":
                entry["hash"] = sha256_bytes(url.encode())
                save_catalogue_entry(idx, entry)
                continue

            try:
                wf = fetch_json(owner, repo, path, branch)
            except Exception as e:
                print(f"[warn] failed to fetch {owner}/{repo}:{path}: {e}", file=sys.stderr)
                continue

            canon = canonical_json_bytes(wf)
            h = sha256_bytes(canon)
            entry["hash"] = h
            entry["integrations"] = detect_integrations(wf)

            if h not in idx:
                from pathlib import Path as P
                src_dir = INGESTED / "by-source" / f"{owner}-{repo}"
                src_dir.mkdir(parents=True, exist_ok=True)
                out = src_dir / f"{P(path).stem}.{h[:10]}.json"
                out.write_bytes(canon)
                for integ in entry["integrations"]:
                    d = INGESTED / "by-integration" / integ
                    d.mkdir(parents=True, exist_ok=True)
                    (d / f"{P(path).stem}.{h[:10]}.json").write_bytes(canon)
                entry["stored_at"] = str(out.relative_to(ROOT))

            save_catalogue_entry(idx, entry)

    INDEX.write_text(json.dumps(idx, indent=2) + "\n")

if __name__ == "__main__":
    main()
