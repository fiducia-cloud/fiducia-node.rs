from pathlib import Path

path = Path("src/schedule_runner.rs")
text = path.read_text(encoding="utf-8")
old = 'Ok(value) if value.as_bytes().len() >= MIN_WEBHOOK_SIGNING_SECRET_BYTES => {'
new = 'Ok(value) if value.len() >= MIN_WEBHOOK_SIGNING_SECRET_BYTES => {'
if text.count(old) != 1:
    raise RuntimeError(f"expected one clippy target, found {text.count(old)}")
path.write_text(text.replace(old, new, 1), encoding="utf-8")
