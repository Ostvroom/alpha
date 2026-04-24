/**
 * Free members: every 4h, optional community tweet attestation (honor-system checkboxes).
 * Premium (Discord role): skipped. Requires NA_COMMUNITY_ENGAGE_TWEET_URL on server + /api/me.engage_tweet_url.
 */
(function () {
  var STORAGE = "na_community_next_ok";
  var PERIOD_MS = 4 * 60 * 60 * 1000;

  function ready(fn) {
    if (document.readyState !== "loading") fn();
    else document.addEventListener("DOMContentLoaded", fn);
  }

  function parseUntil(v) {
    try {
      return parseInt(String(v || "0"), 10) || 0;
    } catch (e) {
      return 0;
    }
  }

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function showModal(tweetUrl) {
    if (document.getElementById("na-pulse-overlay")) return;
    document.body.style.overflow = "hidden";
    var o = document.createElement("div");
    o.id = "na-pulse-overlay";
    o.style.cssText =
      "position:fixed;inset:0;z-index:10000;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,.78);padding:16px;";
    o.innerHTML =
      '<div style="width:min(520px,100%);background:#0b0b0d;border:1px solid rgba(255,204,0,.35);border-radius:14px;padding:18px 18px 14px;box-shadow:0 24px 80px rgba(0,0,0,.6);font-family:Inter,system-ui,sans-serif;color:#f0e8c0;">' +
      '<div style="font-family:JetBrains Mono,monospace;font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:#ffcc00;">Support Nerds Alpha</div>' +
      '<p style="margin:12px 0 10px;font-size:14px;line-height:1.5;color:rgba(240,232,192,.82);">Free access is supported by the community. Every few hours, open our post on X, <b>like</b> and <b>repost</b> it, then confirm below. <span style="color:rgba(240,232,192,.55);">Paid members never see this.</span></p>' +
      '<div style="margin:12px 0;"><a id="na-pulse-tweet" href="' +
      esc(tweetUrl) +
      '" target="_blank" rel="noreferrer" style="display:inline-flex;padding:10px 14px;border-radius:10px;border:1px solid rgba(255,204,0,.35);color:#ffcc00;font-family:JetBrains Mono,monospace;font-size:11px;letter-spacing:.1em;text-transform:uppercase;">Open post on X</a></div>' +
      '<label style="display:flex;gap:10px;align-items:center;margin:10px 0;font-size:13px;cursor:pointer;"><input type="checkbox" id="na-pulse-like" /> I liked the post</label>' +
      '<label style="display:flex;gap:10px;align-items:center;margin:10px 0;font-size:13px;cursor:pointer;"><input type="checkbox" id="na-pulse-rt" /> I reposted / retweeted</label>' +
      '<button type="button" id="na-pulse-done" style="margin-top:14px;width:100%;padding:12px;border-radius:10px;border:1px solid rgba(255,204,0,.28);background:rgba(255,204,0,.10);color:#ffcc00;font-family:JetBrains Mono,monospace;font-size:11px;letter-spacing:.12em;text-transform:uppercase;cursor:pointer;">Continue to site</button>' +
      "</div>";
    document.body.appendChild(o);
    var like = o.querySelector("#na-pulse-like");
    var rt = o.querySelector("#na-pulse-rt");
    var btn = o.querySelector("#na-pulse-done");
    if (btn) {
      btn.addEventListener("click", function () {
        if (!like || !like.checked || !rt || !rt.checked) return;
        try {
          localStorage.setItem(STORAGE, String(Date.now() + PERIOD_MS));
        } catch (e) {}
        o.remove();
        document.body.style.overflow = "";
      });
    }
  }

  async function run() {
    if (window.__NA_COMMUNITY_PULSE__) return;
    window.__NA_COMMUNITY_PULSE__ = true;
    if (document.body && document.body.classList.contains("locked")) return;
    if (window.NA_NEEDS_GATE === true) return;
    var r;
    try {
      r = await fetch("/api/me", { credentials: "include" });
    } catch (e) {
      return;
    }
    if (!r || !r.ok) return;
    var me;
    try {
      me = await r.json();
    } catch (e) {
      return;
    }
    if (!me || me.is_premium) return;
    var tweet = String(me.engage_tweet_url || "").trim();
    if (!tweet) return;
    if (Date.now() < parseUntil(localStorage.getItem(STORAGE))) return;
    showModal(tweet);
  }

  ready(function () {
    setTimeout(run, 1600);
  });
})();
