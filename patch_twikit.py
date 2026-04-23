"""
Patches the installed twikit library to fix bugs caused by
Twitter's JS no longer containing the expected index patterns:

  Bug 1 (0 indices found):
    Exception: Couldn't get KEY_BYTE indices

  Bug 2 (1 index found, empty tail list):
    TypeError: reduce() of empty iterable with no initial value

  Bug 3: get_animation_key crash when DEFAULT_KEY_BYTES_INDICES is []
    because reduce() over empty list even WITH initial value=1 still
    fails when key_bytes list-comp produces [] and row_index becomes 0
    but frames may be empty.

Strategy — TWO-LAYER defence so pycache issues on Render can't bypass us:
  Layer 1 (file patch): rewrite transaction.py on disk + wipe ALL pycache.
  Layer 2 (monkey-patch): replace the live Python objects in memory so
    even if the file patch is skipped the running process is fixed.
"""
import os
import re
import sys
import shutil


# ── helpers ──────────────────────────────────────────────────────────────────

def _clear_pycache_dir(directory):
    """Nuke the entire __pycache__ dir so stale .pyc can't shadow our patch."""
    cache_dir = os.path.join(directory, "__pycache__")
    if os.path.isdir(cache_dir):
        try:
            shutil.rmtree(cache_dir)
        except Exception:
            # fallback: delete file by file
            for fname in os.listdir(cache_dir):
                try:
                    os.remove(os.path.join(cache_dir, fname))
                except Exception:
                    pass


def _clear_pycache(py_path):
    """Clear pycache next to a specific .py file."""
    _clear_pycache_dir(os.path.dirname(py_path))


# ── Layer 1: file-level patch ─────────────────────────────────────────────────

def apply_patch():
    try:
        import twikit
    except ImportError:
        print("    [PATCH] twikit not installed yet - skipping.")
        return

    path = os.path.join(
        os.path.dirname(twikit.__file__),
        "x_client_transaction",
        "transaction.py"
    )

    if not os.path.exists(path):
        print(f"    [PATCH] transaction.py not found at: {path}")
        _apply_monkey_patch()
        return

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    original = content
    changed = False

    # ── Fix 1: "Couldn't get KEY_BYTE indices" (original raise, pre-2.3.3) ──
    old1 = (
        "        if not key_byte_indices:\n"
        "            raise Exception(\"Couldn't get KEY_BYTE indices\")\n"
        "        key_byte_indices = list(map(int, key_byte_indices))\n"
        "        return key_byte_indices[0], key_byte_indices[1:]"
    )
    new1 = (
        "        key_byte_indices = list(map(int, key_byte_indices))\n"
        "        if not key_byte_indices:\n"
        "            return 0, []  # fallback: Twitter JS changed, use safe defaults\n"
        "        return key_byte_indices[0], key_byte_indices[1:]"
    )
    if old1 in content:
        content = content.replace(old1, new1)
        changed = True

    # ── Fix 1b: regex fallback for any whitespace/layout variant ─────────────
    if "Couldn't get KEY_BYTE indices" in content:
        new_content, n = re.subn(
            r"\braise Exception\(\s*[\"']Couldn't get KEY_BYTE indices[\"']\s*\)",
            "return 0, []  # patched KEY_BYTE drift",
            content,
        )
        if n:
            content = new_content
            changed = True

    # ── Fix 2: reduce() without initializer (pre-2.3.3 single-line form) ─────
    old_reduce = "[key_bytes[index] % 16 for index in self.DEFAULT_KEY_BYTES_INDICES])"
    new_reduce = "[key_bytes[index] % 16 for index in self.DEFAULT_KEY_BYTES_INDICES], 1)"
    if old_reduce in content and new_reduce not in content:
        content = content.replace(old_reduce, new_reduce, 1)
        changed = True

    # ── Fix 3: Remove leftover DEBUG print statements ─────────────────────────
    old3 = (
        "                print(f\"DEBUG: Fetching {on_demand_file_url}\")\n"
        "                on_demand_file_response = await session.request(method=\"GET\", url=on_demand_file_url, headers=headers)\n"
        "                key_byte_indices_match = INDICES_REGEX.finditer(\n"
        "                    str(on_demand_file_response.text))\n"
        "                for item in key_byte_indices_match:\n"
        "                    print(f\"DEBUG: Found index {item.group(2)}\")\n"
        "                    key_byte_indices.append(item.group(2))\n"
        "                if key_byte_indices:\n"
        "                    print(f\"DEBUG: Successfully found indices in {on_demand_file_url}\")\n"
        "                    break\n"
        "            except Exception as e:\n"
        "                print(f\"DEBUG: Failed to fetch/parse {on_demand_file_url}: {e}\")\n"
        "                continue\n"
    )
    new3 = (
        "                on_demand_file_response = await session.request(method=\"GET\", url=on_demand_file_url, headers=headers)\n"
        "                key_byte_indices_match = INDICES_REGEX.finditer(\n"
        "                    str(on_demand_file_response.text))\n"
        "                for item in key_byte_indices_match:\n"
        "                    key_byte_indices.append(item.group(2))\n"
        "                if key_byte_indices:\n"
        "                    break\n"
        "            except Exception:\n"
        "                continue\n"
    )
    if old3 in content:
        content = content.replace(old3, new3)
        changed = True

    if changed:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        # Wipe the ENTIRE __pycache__ for x_client_transaction so stale .pyc
        # can't shadow the freshly written .py file on next import.
        _clear_pycache(path)
        print("    [PATCH] twikit transaction.py fixes applied + pycache cleared.")
    else:
        print("    [PATCH] transaction.py already up to date (file layer).")

    apply_patch_user_entities()

    # Always apply the in-memory monkey-patch on top.
    _apply_monkey_patch()


# ── Layer 2: in-memory monkey-patch ──────────────────────────────────────────

def _apply_monkey_patch():
    """
    Replace the live ClientTransaction.get_indices and get_animation_key
    with safe versions — works even when pycache hides the file-level patch.

    Handles:
      • get_indices returning (0, []) when Twitter JS has no matching indices
      • get_animation_key not crashing when DEFAULT_KEY_BYTES_INDICES is []
    """
    try:
        from twikit.x_client_transaction.transaction import ClientTransaction
    except Exception as e:
        print(f"    [PATCH] monkey-patch: could not import ClientTransaction: {e}")
        return

    # ── patch get_indices ───────────────────────────────────────────────────
    original_get_indices = ClientTransaction.get_indices

    async def _safe_get_indices(self, home_page_response, session, headers):
        try:
            result = await original_get_indices(self, home_page_response, session, headers)
            return result
        except Exception as e:
            msg = str(e)
            if "KEY_BYTE" in msg or "reduce" in msg or "indices" in msg.lower():
                print(f"    [PATCH] get_indices suppressed: {msg} → using (0, [])")
                return 0, []
            raise

    ClientTransaction.get_indices = _safe_get_indices

    # ── patch get_animation_key ─────────────────────────────────────────────
    original_get_animation_key = ClientTransaction.get_animation_key

    def _safe_get_animation_key(self, key_bytes, response):
        try:
            # If DEFAULT_KEY_BYTES_INDICES is empty/None, frame_time would be 1
            # (safe: reduce with initial=1 over [] = 1). But row_index from
            # key_bytes[0] % 16 could still index an empty frames list —
            # so we guard the whole call.
            return original_get_animation_key(self, key_bytes, response)
        except Exception as e:
            msg = str(e)
            # Swallow errors from empty index lists or empty animation frames
            print(f"    [PATCH] get_animation_key suppressed: {msg} → using '0'")
            return "0"

    ClientTransaction.get_animation_key = _safe_get_animation_key

    # ── patch init to survive fully broken responses ────────────────────────
    original_init = ClientTransaction.init

    async def _safe_init(self, session, headers):
        try:
            await original_init(self, session, headers)
        except Exception as e:
            msg = str(e)
            # Allow partial init — set safe defaults so the client can still work
            if not getattr(self, "DEFAULT_KEY_BYTES_INDICES", None):
                self.DEFAULT_ROW_INDEX = 0
                self.DEFAULT_KEY_BYTES_INDICES = []
            if not getattr(self, "key", None):
                self.key = ""
            if not getattr(self, "key_bytes", None):
                self.key_bytes = []
            if not getattr(self, "animation_key", None):
                self.animation_key = "0"
            print(f"    [PATCH] ClientTransaction.init recovered from: {msg}")

    ClientTransaction.init = _safe_init

    print("    [PATCH] In-memory monkey-patch applied (get_indices, get_animation_key, init).")

    # ── patch User.__init__ ─────────────────────────────────────────────────
    # The user.py file patch fixes the source on disk, but the User class is
    # already loaded in memory on the first deploy. Wrap __init__ so any
    # KeyError on missing 'urls', 'pinned_tweet_ids_str', etc. is recovered.
    _patch_user_class("twikit.user", "User")
    _patch_user_class("twikit.guest.user", "User")


def _patch_user_class(module_path: str, class_name: str):
    """Monkey-patch a twikit User class __init__ to tolerate missing legacy fields."""
    try:
        import importlib
        mod = importlib.import_module(module_path)
        UserClass = getattr(mod, class_name, None)
        if UserClass is None:
            return

        _orig_init = UserClass.__init__

        def _safe_user_init(self, *args, **kwargs):
            try:
                _orig_init(self, *args, **kwargs)
            except (KeyError, TypeError) as e:
                # Set safe defaults for any field that didn't get initialised
                for attr, default in [
                    ("description_urls", []),
                    ("urls", []),
                    ("pinned_tweet_ids", []),
                    ("withheld_in_countries", []),
                ]:
                    if not hasattr(self, attr):
                        setattr(self, attr, default)
                # Re-raise if it's something completely unrelated
                if not any(
                    kw in str(e)
                    for kw in ["urls", "pinned_tweet", "withheld", "entities", "legacy"]
                ):
                    raise

        UserClass.__init__ = _safe_user_init
        print(f"    [PATCH] {module_path}.{class_name}.__init__ safe-guarded.")
    except Exception as e:
        # Non-fatal: file-level patch + twitter_client KeyError handling covers us
        print(f"    [PATCH] Could not patch {module_path}.{class_name}: {e}")


# ── user.py safe-guards ───────────────────────────────────────────────────────

def apply_patch_user_entities():
    """
    X sometimes omits legacy['entities']['description']['urls'] or url.url.urls.
    twikit raises KeyError / NoneType — safe defaults fix session health checks.
    """
    try:
        import twikit
    except ImportError:
        return

    base = os.path.dirname(twikit.__file__)
    for rel in ("user.py", os.path.join("guest", "user.py")):
        path = os.path.join(base, rel)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        changed = False

        old = (
            "        self.description_urls: list = legacy['entities']['description']['urls']\n"
            "        self.urls: list = legacy['entities'].get('url', {}).get('urls')"
        )
        new = (
            "        _ent = legacy.get('entities') or {}\n"
            "        _desc = _ent.get('description') if isinstance(_ent.get('description'), dict) else {}\n"
            "        self.description_urls: list = (_desc.get('urls') if isinstance(_desc, dict) else None) or []\n"
            "        _url_block = _ent.get('url')\n"
            "        self.urls: list = (_url_block.get('urls') if isinstance(_url_block, dict) else None) or []"
        )
        if old in content:
            content = content.replace(old, new, 1)
            changed = True

        # pinned_tweet_ids_str is sometimes missing — avoid KeyError
        old_pin = "        self.pinned_tweet_ids: list[str] = legacy['pinned_tweet_ids_str']"
        new_pin = "        self.pinned_tweet_ids: list[str] = legacy.get('pinned_tweet_ids_str') or []"
        if old_pin in content:
            content = content.replace(old_pin, new_pin, 1)
            changed = True

        # withheld_in_countries is sometimes missing — avoid KeyError
        old_withheld = "        self.withheld_in_countries: list[str] = legacy['withheld_in_countries']"
        new_withheld = "        self.withheld_in_countries: list[str] = legacy.get('withheld_in_countries') or []"
        if old_withheld in content:
            content = content.replace(old_withheld, new_withheld, 1)
            changed = True

        if not changed:
            continue
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        _clear_pycache(path)
        print(f"    [PATCH] twikit user parsing safe-guards -> {rel}")


if __name__ == "__main__":
    apply_patch()
