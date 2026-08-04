from pathlib import Path

path = Path("src/state.rs")
text = path.read_text()
block = '''            if self.locks.grants.values().any(|grant| {
                grant.holder == queued.holder && grant.keys == queued.keys
            }) {
                return Err("the same union-lock identity is both granted and queued".to_string());
            }
'''
count = text.count(block)
if count != 1:
    raise SystemExit(
        f"expected one over-strong grant-plus-queue recovery check, found {count}"
    )
path.write_text(text.replace(block, "", 1))
