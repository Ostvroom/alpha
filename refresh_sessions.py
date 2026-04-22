import json
import os
import sys

def refresh_cookies():
    print("====================================================")
    print("🛡️ Twitter Session Refresh Utility")
    print("====================================================")
    print("\nThis script helps you update your 'cookies.json' to keep the bot running.")
    print("\nHow to get your cookies:")
    print("1. Open Twitter (x.com) in your Chrome/Edge browser and login.")
    print("2. Use an extension like 'EditThisCookie' or 'Cookie-Editor'.")
    print("3. Click 'Export' or 'Copy' as JSON.")
    print("4. Paste the content here when prompted.")
    
    print("\n--- PASTE JSON COOKIES BELOW (Press Enter + Ctrl+Z on Windows etc to finish) ---")
    print("(Or just paste and press Enter)")
    
    try:
        # Read multiline input
        lines = []
        while True:
            try:
                line = input()
                if not line and lines: # Empty line after some content ends it
                    break
                lines.append(line)
            except EOFError:
                break
        
        raw_data = "".join(lines).strip()
        if not raw_data:
            print("❌ No input detected.")
            return

        # Attempt to parse as JSON
        try:
            cookies = json.loads(raw_data)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON format: {e}")
            return

        # Check if it's a list (standard for most cookie exporters)
        if not isinstance(cookies, list):
            print("⚠️ Warning: Paste doesn't look like a standard cookie list (JSON Array).")
            print("Attempting to save anyway...")

        # Save to cookies.json
        target = os.path.join(os.path.dirname(__file__), "cookies.json")
        with open(target, "w") as f:
            json.dump(cookies, f, indent=4)
        
        print(f"\n✅ SUCCESS: cookies.json has been updated at {target}")
        print("🚀 You can now restart the bot.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    refresh_cookies()
