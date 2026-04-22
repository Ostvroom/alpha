"""
Patches the installed twikit library to fix two related bugs caused by
Twitter's JS no longer containing the expected index patterns:

  Bug 1 (0 indices found):
    Exception: Couldn't get KEY_BYTE indices

  Bug 2 (1 index found, empty tail list):
    TypeError: reduce() of empty iterable with no initial value

Both are fixed by:
  - Returning safe fallback values (0, []) when no indices are found
  - Adding 1 as the reduce() initial value so an empty list doesn't crash
"""
import os
import sys


def _clear_pycache(py_path):
    cache_dir = os.path.join(os.path.dirname(py_path), "__pycache__")
    if os.path.isdir(cache_dir):
        for fname in os.listdir(cache_dir):
            if "transaction" in fname:
                try:
                    os.remove(os.path.join(cache_dir, fname))
                except Exception:
                    pass


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
        return

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    original = content
    changed = False

    # ── Fix 1: "Couldn't get KEY_BYTE indices" (0 indices found) ─────────────
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

    # ── Fix 2: reduce() on empty list (1 index found, tail is []) ────────────
    old2 = (
        "[key_bytes[index] % 16 for index in self.DEFAULT_KEY_BYTES_INDICES])"
    )
    new2 = (
        "[key_bytes[index] % 16 for index in self.DEFAULT_KEY_BYTES_INDICES], 1)"
    )
    if old2 in content:
        content = content.replace(old2, new2)
        changed = True

    # ── Fix 3: Remove DEBUG print statements from the JS-fetching loop ───────
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
        _clear_pycache(path)
        print("    [PATCH] twikit fixes applied successfully.")
    else:
        print("    [PATCH] Already up to date.")

    apply_patch_user_entities()


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
        changed = False
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
