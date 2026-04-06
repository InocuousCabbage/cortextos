#!/usr/bin/env python3
"""
Production Daily YouTube Pipeline
Combines channel scan (Apify) + keyword discovery (yt-dlp) + tiered transcription.

Usage:
  python3 pipeline.py                    # Full pipeline (channel scan + discovery + transcribe)
  python3 pipeline.py --discovery-only   # Skip channel scan, just keyword discovery
  python3 pipeline.py --scan-only        # Skip discovery, just channel scan
  python3 pipeline.py --no-transcribe    # Skip transcription step
  python3 pipeline.py --hours 48         # Discovery lookback window (default: 168 = 7 days)
  python3 pipeline.py --test N           # Test mode: only process N terms

Schedule: Daily at 00:41 via /loop cron
Runtime: ~20-25 min with rate limiting delays
Cost: Near zero (yt-dlp + captions + Whisper free). Apify only for channel scan + tier 3 fallback.
"""

import json
import os
import sys
import time
import subprocess
import re
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_DIR = os.path.join(SCRIPT_DIR, ".state")
KEYWORDS_FILE = os.path.join(STATE_DIR, "keywords.json")
REGISTRY_FILE = os.path.join(STATE_DIR, "video_registry.json")
CHANNELS_FILE = os.path.join(STATE_DIR, "channels.json")
TRANSCRIPT_DIR = os.path.join(SCRIPT_DIR, "transcripts")
DIGEST_DIR = os.path.join(SCRIPT_DIR, "digests")
KB_BASE = os.path.join(SCRIPT_DIR, "data")
MANUAL_QUEUE_FILE = os.path.join(SCRIPT_DIR, "manual-queue.txt")  # Fix 2: operator fallback

# Rate limiting
YTDLP_DELAY = 2.5  # seconds between yt-dlp metadata fetches
TRANSCRIPT_DELAY = 2.0  # seconds between transcript API calls
APIFY_CHANNEL_ACTOR = "streamers/youtube-channel-scraper"
APIFY_CHANNEL_MAX = 5  # videos per channel
APIFY_TRANSCRIPT_ACTOR = "supreme_coder/youtube-transcript-scraper"
APIFY_TWITTER_PROFILE_ACTOR = "quacker/twitter-scraper"
APIFY_TWITTER_SEARCH_ACTOR = "apidojo/tweet-scraper"
TWITTER_CONFIG_FILE = os.path.join(STATE_DIR, "twitter-config.json")
TWITTER_REGISTRY_FILE = os.path.join(STATE_DIR, "twitter_registry.json")


def load_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default if default is not None else {}


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ============================================================
# CHANNEL SCAN (Apify - 1 call for all channels)
# ============================================================

def parse_relative_date(date_str):
    """Parse Apify relative dates like '2 days ago' into ISO format."""
    if not date_str:
        return None
    now = datetime.now(timezone.utc)
    date_str = date_str.strip().lower()
    if "T" in date_str or (len(date_str) > 8 and "-" in date_str):
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00")).isoformat()
        except ValueError:
            pass
    patterns = [
        (r"(\d+)\s*minute", "minutes"), (r"(\d+)\s*hour", "hours"),
        (r"(\d+)\s*day", "days"), (r"(\d+)\s*week", "weeks"),
        (r"(\d+)\s*month", "months"), (r"(\d+)\s*year", "years"),
    ]
    for pattern, unit in patterns:
        match = re.search(pattern, date_str)
        if match:
            n = int(match.group(1))
            deltas = {"minutes": timedelta(minutes=n), "hours": timedelta(hours=n),
                      "days": timedelta(days=n), "weeks": timedelta(weeks=n),
                      "months": timedelta(days=n*30), "years": timedelta(days=n*365)}
            return (now - deltas[unit]).isoformat()
    cleaned = re.sub(r"^(streamed|premiered)\s+", "", date_str)
    if cleaned != date_str:
        return parse_relative_date(cleaned)
    return now.isoformat()


def channel_scan_ytdlp(channels):
    """Fix 3: yt-dlp fallback for channel scan when Apify is unavailable.
    Uses --flat-playlist to get 5 most recent videos per channel. Free, no API key."""
    videos = []
    for ch in channels:
        handle = ch["handle"].lstrip("@")
        url = f"https://www.youtube.com/@{handle}/videos"
        try:
            result = subprocess.run(
                ["python3", "-m", "yt_dlp", url,
                 "--flat-playlist", "--dump-json", "--no-warnings", "--quiet",
                 "--playlist-end", "5"],
                capture_output=True, text=True, timeout=60
            )
            for line in result.stdout.strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    item = json.loads(line)
                    vid_id = item.get("id", "")
                    if not vid_id:
                        continue
                    # yt-dlp flat-playlist gives upload_date as YYYYMMDD
                    upload_date = item.get("upload_date", "")
                    published = None
                    if upload_date:
                        try:
                            published = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc).isoformat()
                        except ValueError:
                            pass
                    videos.append({
                        "video_id": vid_id,
                        "title": item.get("title", ""),
                        "published": published or datetime.now(timezone.utc).isoformat(),
                        "description": "",
                        "thumbnail": item.get("thumbnail", ""),
                        "view_count": item.get("view_count"),
                        "duration": item.get("duration"),
                        "creator": ch["name"],
                        "creator_handle": handle.lower(),
                        "channel_id": item.get("channel_id", ch.get("channel_id", "")),
                        "source": "channel_scan_ytdlp",
                    })
                except json.JSONDecodeError:
                    continue
        except subprocess.TimeoutExpired:
            print(f"  WARN: yt-dlp channel scan timed out for @{handle}", file=sys.stderr)
        except Exception as e:
            print(f"  WARN: yt-dlp channel scan failed for @{handle}: {e}", file=sys.stderr)
        time.sleep(1)  # polite delay between channels

    print(f"  yt-dlp channel fallback: {len(videos)} videos from {len(channels)} channels")
    return videos


def channel_scan(channels, cutoff):
    """Scan tracked channels via Apify. Returns (list of new video dicts, apify_failed bool)."""
    start_urls = []
    handle_lookup = {}
    for ch in channels:
        handle = ch["handle"].lstrip("@")
        start_urls.append({"url": f"https://www.youtube.com/@{handle}/videos"})
        handle_lookup[handle.lower()] = ch

    input_json = json.dumps({"startUrls": start_urls, "maxResults": APIFY_CHANNEL_MAX})
    apify_output = None
    try:
        result = subprocess.run(
            ["apify", "call", APIFY_CHANNEL_ACTOR, f"--input={input_json}", "--output-dataset"],
            capture_output=True, text=True, timeout=600
        )
        apify_output = result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        print("  ERROR: Apify channel scan timed out (>600s) — falling back to yt-dlp", file=sys.stderr)

    # Fix 3: fall back to yt-dlp if Apify returned nothing usable
    if apify_output is None or "[{" not in apify_output:
        if apify_output is not None:
            print("  ERROR: No JSON in Apify channel scan output — falling back to yt-dlp", file=sys.stderr)
        return channel_scan_ytdlp(channels), True  # True = apify_failed

    json_str = apify_output[apify_output.find("[{"):]
    depth, end_pos = 0, 0
    for i, ch in enumerate(json_str):
        if ch == "[": depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0: end_pos = i + 1; break
    if end_pos == 0:
        print("  ERROR: Could not parse Apify JSON — falling back to yt-dlp", file=sys.stderr)
        return channel_scan_ytdlp(channels), True

    try:
        data = json.loads(json_str[:end_pos])
    except json.JSONDecodeError:
        print("  ERROR: Apify JSON decode failed — falling back to yt-dlp", file=sys.stderr)
        return channel_scan_ytdlp(channels), True

    videos = []
    for item in data:
        vid_id = item.get("id", "")
        if not vid_id:
            continue
        ch_handle = (item.get("channelUsername") or "").lstrip("@").lower()
        ch_info = handle_lookup.get(ch_handle, {})
        published = parse_relative_date(item.get("date", ""))

        videos.append({
            "video_id": vid_id, "title": item.get("title", ""),
            "published": published, "description": "",
            "thumbnail": item.get("thumbnailUrl", ""),
            "view_count": item.get("viewCount"),
            "duration": item.get("duration"),
            "creator": item.get("channelName", ch_info.get("name", "")),
            "creator_handle": ch_handle,
            "channel_id": item.get("channelId", ch_info.get("channel_id", "")),
            "source": "channel_scan",
        })

    print(f"  Channel scan: {len(videos)} videos from {len(set(v['creator_handle'] for v in videos))} channels")
    return videos, False  # False = apify succeeded


# ============================================================
# KEYWORD DISCOVERY (yt-dlp - free, no API key)
# ============================================================

def ytdlp_search(query, max_results=15):
    """Search YouTube via yt-dlp. Returns list of video URLs."""
    try:
        result = subprocess.run(
            ["python3", "-m", "yt_dlp",
             f"ytsearch{max_results}:{query}",
             "--flat-playlist", "--dump-json", "--no-warnings", "--quiet"],
            capture_output=True, text=True, timeout=60
        )
        videos = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                videos.append({
                    "video_id": data.get("id", ""),
                    "title": data.get("title", ""),
                    "url": data.get("url", f"https://www.youtube.com/watch?v={data.get('id', '')}"),
                    "channel": data.get("channel", data.get("uploader", "")),
                    "channel_id": data.get("channel_id", ""),
                    "duration": data.get("duration"),
                    "view_count": data.get("view_count"),
                })
            except json.JSONDecodeError:
                continue
        return videos
    except subprocess.TimeoutExpired:
        print(f"  WARN: yt-dlp search timed out for '{query}'", file=sys.stderr)
        return []


def ytdlp_metadata(video_id):
    """Fetch full metadata for a single video via yt-dlp --dump-json."""
    try:
        result = subprocess.run(
            ["python3", "-m", "yt_dlp",
             f"https://www.youtube.com/watch?v={video_id}",
             "--dump-json", "--no-warnings", "--quiet", "--no-download"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except (subprocess.TimeoutExpired, json.JSONDecodeError):
        return None


def keyword_discovery(terms, max_per_term, cutoff, registry, tracked_channel_ids, tracked_channel_names, debug_dedup=False):
    """Run keyword discovery via yt-dlp. Returns list of new video dicts + stats."""
    all_videos = []
    seen_ids = set()
    stats = {"terms": {}, "filtered": {"dedup_registry": 0, "dedup_batch": 0,
             "tracked_channel": 0, "old": 0, "short": 0, "no_id": 0},
             "consecutive_zero_terms": 0, "discovery_error": False}  # Fix 4
    consecutive_zeros = 0  # Fix 4: track consecutive zero-result search terms

    for i, term in enumerate(terms):
        print(f"\n[{i+1}/{len(terms)}] Searching: '{term}'...")
        results = ytdlp_search(term, max_per_term)
        term_passed = 0

        for video in results:
            vid_id = video.get("video_id", "")
            if not vid_id:
                stats["filtered"]["no_id"] += 1
                continue
            if vid_id in registry.get("videos", {}):
                # Fix 5: debug-dedup — log what registry entry matched
                if debug_dedup and stats["filtered"]["dedup_registry"] < 5:
                    reg_entry = registry["videos"].get(vid_id, {})
                    print(f"  [DEBUG-DEDUP] Skipping {vid_id} '{video.get('title','')[:50]}' — matches registry entry: '{reg_entry.get('title','')[:50]}' scraped={reg_entry.get('scraped_at','?')[:10]}", file=sys.stderr)
                stats["filtered"]["dedup_registry"] += 1
                continue
            if vid_id in seen_ids:
                stats["filtered"]["dedup_batch"] += 1
                continue
            if video.get("channel_id", "") in tracked_channel_ids:
                stats["filtered"]["tracked_channel"] += 1
                continue
            ch_name = (video.get("channel", "") or "").lower()
            if ch_name in tracked_channel_names:
                stats["filtered"]["tracked_channel"] += 1
                continue

            # Fetch full metadata for date + view count
            time.sleep(YTDLP_DELAY)
            meta = ytdlp_metadata(vid_id)
            if not meta:
                continue

            # Date filter
            upload_date = meta.get("upload_date", "")  # YYYYMMDD format
            if upload_date:
                try:
                    pub_dt = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc)
                    if pub_dt < cutoff:
                        stats["filtered"]["old"] += 1
                        continue
                    published = pub_dt.isoformat()
                except ValueError:
                    published = datetime.now(timezone.utc).isoformat()
            else:
                published = datetime.now(timezone.utc).isoformat()

            # Duration filter (skip shorts < 60s)
            duration = meta.get("duration", 60) or 60
            if duration < 60:
                stats["filtered"]["short"] += 1
                continue

            seen_ids.add(vid_id)
            term_passed += 1
            all_videos.append({
                "video_id": vid_id,
                "title": meta.get("title", video.get("title", "")),
                "published": published,
                "description": (meta.get("description", "") or "")[:500],
                "thumbnail": meta.get("thumbnail", ""),
                "view_count": meta.get("view_count"),
                "duration": duration,
                "creator": meta.get("channel", meta.get("uploader", "")),
                "creator_handle": meta.get("uploader_id", ""),
                "channel_id": meta.get("channel_id", ""),
                "source": "discovery",
                "search_term": term,
            })

        stats["terms"][term] = {"raw": len(results), "passed": term_passed}
        print(f"  Raw: {len(results)}, Passed: {term_passed}")

        # Fix 4: track consecutive zero-result terms (raw == 0 = yt-dlp likely rate-limited)
        if len(results) == 0:
            consecutive_zeros += 1
        else:
            consecutive_zeros = 0

        if consecutive_zeros >= 10 and not stats["discovery_error"]:
            print(f"  WARN: {consecutive_zeros} consecutive zero-result terms — yt-dlp may be rate-limited or blocked", file=sys.stderr)
            stats["discovery_error"] = True
            stats["consecutive_zero_terms"] = consecutive_zeros

    print(f"\nDiscovery total: {len(all_videos)} unique videos from {len(terms)} terms")
    if stats["discovery_error"]:
        print(f"  WARNING: discovery_error flagged — {stats['consecutive_zero_terms']} consecutive zero-result terms", file=sys.stderr)
    return all_videos, stats


# ============================================================
# TIERED TRANSCRIPTION
# ============================================================

def transcribe_tier1(video_id):
    """Tier 1: youtube-transcript-api (free captions)."""
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        ytt = YouTubeTranscriptApi()
        transcript = ytt.fetch(video_id)
        segments = [{"start": s.start, "duration": s.duration, "text": s.text} for s in transcript]
        full_text = " ".join(s["text"] for s in segments)
        return {"segments": segments, "full_text": full_text,
                "word_count": len(full_text.split()), "tier": 1}
    except Exception as e:
        err = str(e).lower()
        if "blocking" in err or "ip" in err:
            return {"error": "ip_blocked", "tier": 1}
        return {"error": str(e)[:100], "tier": 1}


def transcribe_tier2(video_id):
    """Tier 2: yt-dlp audio download + Whisper base model."""
    import tempfile
    audio_path = os.path.join(tempfile.gettempdir(), f"{video_id}.mp3")
    try:
        # Download audio
        result = subprocess.run(
            ["python3", "-m", "yt_dlp",
             f"https://www.youtube.com/watch?v={video_id}",
             "-x", "--audio-format", "mp3", "-o", audio_path,
             "--no-warnings", "--quiet"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0 or not os.path.exists(audio_path):
            return {"error": "download_failed", "tier": 2}

        # Transcribe with Whisper base
        import whisper
        model = whisper.load_model("base")
        result = model.transcribe(audio_path)
        full_text = result.get("text", "")
        segments = [{"start": s["start"], "duration": s["end"] - s["start"], "text": s["text"]}
                    for s in result.get("segments", [])]

        return {"segments": segments, "full_text": full_text,
                "word_count": len(full_text.split()), "tier": 2}
    except Exception as e:
        return {"error": str(e)[:100], "tier": 2}
    finally:
        if os.path.exists(audio_path):
            os.remove(audio_path)


def transcribe_tier3(video_id):
    """Tier 3: Apify transcript scraper (paid fallback)."""
    url = f"https://www.youtube.com/watch?v={video_id}"
    input_json = json.dumps({"urls": [{"url": url}]})
    try:
        result = subprocess.run(
            ["apify", "call", APIFY_TRANSCRIPT_ACTOR,
             "-i", input_json, "-t", "180", "-o", "-s"],
            capture_output=True, text=True, timeout=240
        )
        output = result.stdout.strip()
        if not output:
            return {"error": "no_output", "tier": 3}
        data = json.loads(output)
        if not data:
            return {"error": "empty_result", "tier": 3}
        t = data[0] if isinstance(data, list) else data
        segments = t.get("transcript", [])
        full_text = " ".join(s.get("text", "") for s in segments)
        return {"segments": segments, "full_text": full_text,
                "word_count": len(full_text.split()), "tier": 3}
    except Exception as e:
        return {"error": str(e)[:100], "tier": 3}


def transcribe_video(video_id):
    """Tiered transcription: captions -> Whisper -> Apify."""
    # Tier 1: Free captions
    result = transcribe_tier1(video_id)
    if "full_text" in result and result["word_count"] > 0:
        return result

    # Tier 2: Whisper (if tier 1 failed, not just IP blocked)
    if result.get("error") != "ip_blocked":
        time.sleep(1)
        result = transcribe_tier2(video_id)
        if "full_text" in result and result["word_count"] > 0:
            return result

    # Tier 3: Apify (paid fallback)
    result = transcribe_tier3(video_id)
    if "full_text" in result and result["word_count"] > 0:
        return result

    return {"error": "all_tiers_failed", "tier": 0, "word_count": 0}


def save_transcript(video_id, transcript_data, video_meta):
    """Save transcript to both flat dir and KB dir."""
    os.makedirs(TRANSCRIPT_DIR, exist_ok=True)
    creator_slug = (video_meta.get("creator") or "unknown").lower().replace(" ", "-")
    creator_slug = re.sub(r"[^a-z0-9\-]", "", creator_slug)

    doc = {
        "video_id": video_id,
        "title": video_meta.get("title", ""),
        "creator": video_meta.get("creator", ""),
        "published": video_meta.get("published", ""),
        "description": video_meta.get("description", ""),
        "segments": transcript_data.get("segments", []),
        "full_text": transcript_data.get("full_text", ""),
        "word_count": transcript_data.get("word_count", 0),
        "transcription_tier": transcript_data.get("tier", 0),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }

    # Save to flat transcripts/ dir
    save_json(os.path.join(TRANSCRIPT_DIR, f"{video_id}.json"), doc)

    # Save to KB dir
    kb_dir = os.path.join(KB_BASE, creator_slug, "transcripts")
    os.makedirs(kb_dir, exist_ok=True)
    save_json(os.path.join(kb_dir, f"{video_id}.json"), doc)


# ============================================================
# X/TWITTER SCAN (Apify)
# ============================================================

def twitter_scan(config, cutoff, twitter_registry):
    """Scan X/Twitter accounts and search terms via Apify. Returns list of tweet dicts."""
    accounts = config.get("accounts", [])
    search_terms = config.get("search_terms", [])
    min_likes = config.get("min_likes", 50)
    min_replies = config.get("min_replies", 10)
    max_per_account = config.get("max_tweets_per_account", 10)
    max_per_search = config.get("max_tweets_per_search", 20)

    all_tweets = []
    seen_ids = set(twitter_registry.get("tweet_ids", []))

    def _parse_apify_json_array(output):
        """Parse JSON array from Apify CLI output, handling large outputs."""
        start = output.find("[")
        if start == -1:
            return []
        # Find the last ] in the output
        end = output.rfind("]")
        if end == -1 or end <= start:
            return []
        try:
            return json.loads(output[start:end + 1])
        except json.JSONDecodeError:
            # Try to find the array more carefully
            json_str = output[start:]
            depth, end_pos = 0, 0
            for i, ch in enumerate(json_str):
                if ch == "[": depth += 1
                elif ch == "]":
                    depth -= 1
                    if depth == 0: end_pos = i + 1; break
            if end_pos > 0:
                try:
                    return json.loads(json_str[:end_pos])
                except json.JSONDecodeError:
                    pass
            return []

    # Scrape accounts (batch into groups of 3 to avoid timeouts)
    if accounts:
        batch_size = 3
        for batch_start in range(0, len(accounts), batch_size):
            batch = accounts[batch_start:batch_start + batch_size]
            print(f"  Scraping accounts batch {batch_start//batch_size + 1}: {', '.join(batch)}...")
            input_json = json.dumps({
                "startUrls": [{"url": f"https://x.com/{a.lstrip('@')}"} for a in batch],
                "maxTweets": max_per_account,
                "tweetsDesired": max_per_account * len(batch),
            })
            try:
                result = subprocess.run(
                    ["apify", "call", APIFY_TWITTER_PROFILE_ACTOR,
                     f"--input={input_json}", "--output-dataset"],
                    capture_output=True, text=True, timeout=300
                )
                output = result.stdout + result.stderr
                tweets = _parse_apify_json_array(output)
                if tweets:
                    all_tweets.extend(tweets)
                    print(f"    Got {len(tweets)} tweets")
                else:
                    print(f"    No tweets returned")
            except subprocess.TimeoutExpired:
                print(f"    WARN: Batch timed out", file=sys.stderr)
            except Exception as e:
                print(f"    WARN: Batch failed: {e}", file=sys.stderr)

    # Search terms (uses apidojo/tweet-scraper which supports searchTerms)
    if search_terms:
        print(f"  Searching {len(search_terms)} terms via tweet-scraper...")
        input_json = json.dumps({
            "searchTerms": search_terms,
            "maxItems": max_per_search * len(search_terms),
            "sort": "Top",
        })
        try:
            result = subprocess.run(
                ["apify", "call", APIFY_TWITTER_SEARCH_ACTOR,
                 f"--input={input_json}", "--output-dataset"],
                capture_output=True, text=True, timeout=300
            )
            output = result.stdout + result.stderr
            tweets = _parse_apify_json_array(output)
            if tweets:
                # Normalize field names to match quacker format
                for t in tweets:
                    if "fullText" in t and "full_text" not in t:
                        t["full_text"] = t["fullText"]
                    if "likeCount" in t and "favorite_count" not in t:
                        t["favorite_count"] = t["likeCount"]
                    if "replyCount" in t and "conversation_count" not in t:
                        t["conversation_count"] = t["replyCount"]
                    if "id" in t and "id_str" not in t:
                        t["id_str"] = str(t["id"])
                    if "createdAt" in t and "created_at" not in t:
                        t["created_at"] = t["createdAt"]
                    if "viewCount" in t and "views" not in t:
                        t["views"] = t["viewCount"]
                all_tweets.extend(tweets)
                print(f"    Got {len(tweets)} tweets from search")
            else:
                print(f"    No search results")
        except subprocess.TimeoutExpired:
            print(f"    WARN: Search timed out", file=sys.stderr)
        except Exception as e:
            print(f"    WARN: Search failed: {e}", file=sys.stderr)

    # Dedup + filter
    new_tweets = []
    dedup_ids = set()
    for t in all_tweets:
        tid = t.get("id_str", "") or str(t.get("id", ""))
        if not tid or tid == "0" or tid in seen_ids or tid in dedup_ids:
            continue
        dedup_ids.add(tid)

        likes = t.get("favorite_count", 0) or 0
        replies = t.get("conversation_count", t.get("reply_count", 0)) or 0
        if likes < min_likes and replies < min_replies:
            continue

        # Parse date
        created = t.get("created_at", "")
        published = created if created else datetime.now(timezone.utc).isoformat()

        text = t.get("full_text", t.get("text", ""))
        # quacker actor nests author info differently
        author = t.get("author", {}) if isinstance(t.get("author"), dict) else {}
        author_name = author.get("name", t.get("user", {}).get("name", "")) if isinstance(t.get("user"), dict) else author.get("name", "")
        author_handle = author.get("userName", author.get("screen_name", ""))
        if not author_handle:
            # Extract from permalink: /handle/status/id
            permalink = t.get("permalink", "")
            if permalink and "/" in permalink:
                author_handle = permalink.strip("/").split("/")[0]

        tweet_url = f"https://x.com/{author_handle}/status/{tid}" if author_handle else ""

        new_tweets.append({
            "id": str(tid),
            "creator": author_name or author_handle,
            "creator_handle": author_handle,
            "title": text[:80] + ("..." if len(text) > 80 else ""),
            "text": text,
            "words": len(text.split()),
            "published": published,
            "view_count": t.get("views", t.get("view_count")),
            "likes": likes,
            "retweets": t.get("retweet_count", 0),
            "replies": replies,
            "url": tweet_url,
            "source": "twitter",
            "platform": "twitter",
        })

    # Update registry
    twitter_registry["tweet_ids"] = list(seen_ids | dedup_ids)
    twitter_registry["last_scan"] = datetime.now(timezone.utc).isoformat()

    print(f"  Twitter: {len(new_tweets)} new tweets after dedup + engagement filter")
    return new_tweets


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Production Daily YouTube Pipeline")
    parser.add_argument("--hours", type=int, default=168, help="Discovery lookback hours (default: 168 = 7 days)")
    parser.add_argument("--scan-hours", type=int, default=24, help="Channel scan lookback hours (default: 24)")
    parser.add_argument("--discovery-only", action="store_true", help="Skip channel scan")
    parser.add_argument("--scan-only", action="store_true", help="Skip discovery")
    parser.add_argument("--no-transcribe", action="store_true", help="Skip transcription")
    parser.add_argument("--no-twitter", action="store_true", help="Skip X/Twitter scan")
    parser.add_argument("--twitter-only", action="store_true", help="Only run X/Twitter scan")
    parser.add_argument("--test", type=int, default=0, help="Test mode: only N discovery terms")
    parser.add_argument("--debug-dedup", action="store_true", help="Fix 5: log first 5 skipped_existing matches for dedup audit")
    args = parser.parse_args()

    channels_data = load_json(CHANNELS_FILE, {"channels": []})
    keywords_data = load_json(KEYWORDS_FILE, {})
    registry = load_json(REGISTRY_FILE, {"videos": {}, "last_scan": None})
    twitter_config = load_json(TWITTER_CONFIG_FILE, {})
    twitter_registry = load_json(TWITTER_REGISTRY_FILE, {"tweet_ids": [], "last_scan": None})

    tracked_ids = {ch["channel_id"] for ch in channels_data.get("channels", [])}
    tracked_names = {ch["name"].lower() for ch in channels_data.get("channels", [])}
    scan_cutoff = datetime.now(timezone.utc) - timedelta(hours=args.scan_hours)
    discovery_cutoff = datetime.now(timezone.utc) - timedelta(hours=args.hours)

    all_new_videos = []
    discovery_stats = {}
    # Fix 1: diagnostic flags for zero-result alert
    apify_channel_failed = False
    discovery_error = False

    # Fix 2: ingest manual-queue.txt (operator fallback for high-signal URLs)
    manual_videos = []
    if os.path.exists(MANUAL_QUEUE_FILE):
        with open(MANUAL_QUEUE_FILE) as f:
            queue_urls = [l.strip() for l in f if l.strip() and not l.startswith("#")]
        if queue_urls:
            print(f"\nMANUAL QUEUE: {len(queue_urls)} URLs")
            for url in queue_urls:
                # Extract video ID from URL
                vid_id_match = re.search(r"(?:v=|youtu\.be/)([A-Za-z0-9_-]{11})", url)
                if not vid_id_match:
                    print(f"  WARN: could not parse video ID from {url}", file=sys.stderr)
                    continue
                vid_id = vid_id_match.group(1)
                if vid_id in registry.get("videos", {}):
                    print(f"  SKIP (already in registry): {vid_id}")
                    continue
                time.sleep(YTDLP_DELAY)
                meta = ytdlp_metadata(vid_id)
                if not meta:
                    print(f"  WARN: could not fetch metadata for {vid_id}", file=sys.stderr)
                    continue
                upload_date = meta.get("upload_date", "")
                published = datetime.now(timezone.utc).isoformat()
                if upload_date:
                    try:
                        published = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=timezone.utc).isoformat()
                    except ValueError:
                        pass
                manual_videos.append({
                    "video_id": vid_id,
                    "title": meta.get("title", ""),
                    "published": published,
                    "description": (meta.get("description", "") or "")[:500],
                    "thumbnail": meta.get("thumbnail", ""),
                    "view_count": meta.get("view_count"),
                    "duration": meta.get("duration", 0),
                    "creator": meta.get("channel", meta.get("uploader", "")),
                    "creator_handle": meta.get("uploader_id", ""),
                    "channel_id": meta.get("channel_id", ""),
                    "source": "manual",
                    "manual_url": url,
                })
                print(f"  QUEUED: [{meta.get('channel','')}] {meta.get('title','')[:60]}")
            all_new_videos.extend(manual_videos)
            print(f"  Manual queue: {len(manual_videos)} new videos added")

    # ---- CHANNEL SCAN ----
    if not args.discovery_only:
        print("=" * 60)
        print("CHANNEL SCAN (Apify)")
        print("=" * 60)
        scan_videos, apify_channel_failed = channel_scan(channels_data["channels"], scan_cutoff)
        skipped = 0
        for v in scan_videos:
            if v["video_id"] in registry.get("videos", {}):
                skipped += 1
                continue
            if v["published"]:
                try:
                    dt = datetime.fromisoformat(v["published"].replace("Z", "+00:00"))
                    if dt < scan_cutoff:
                        skipped += 1
                        continue
                except ValueError:
                    pass
            all_new_videos.append(v)
        print(f"  New after dedup: {len([v for v in all_new_videos if v['source']=='channel_scan'])}, Skipped: {skipped}")

    # ---- KEYWORD DISCOVERY ----
    if not args.scan_only:
        print("\n" + "=" * 60)
        print("KEYWORD DISCOVERY (yt-dlp)")
        print("=" * 60)
        terms = keywords_data.get("search_terms", [])
        if args.test > 0:
            terms = terms[:args.test]
            print(f"  TEST MODE: only {args.test} terms")
        max_per_term = keywords_data.get("max_results_per_term", 15)

        disc_videos, discovery_stats = keyword_discovery(
            terms, max_per_term, discovery_cutoff,
            registry, tracked_ids, tracked_names,
            debug_dedup=args.debug_dedup,
        )
        all_new_videos.extend(disc_videos)
        discovery_error = discovery_stats.get("discovery_error", False)

    # Dedup across scan + discovery
    seen = set()
    deduped = []
    for v in all_new_videos:
        if v["video_id"] not in seen:
            seen.add(v["video_id"])
            deduped.append(v)
    all_new_videos = deduped

    print(f"\n{'=' * 60}")
    print(f"TOTAL NEW VIDEOS: {len(all_new_videos)}")
    print(f"{'=' * 60}")

    # ---- TRANSCRIPTION ----
    transcript_stats = {"tier1": 0, "tier2": 0, "tier3": 0, "failed": 0}
    if not args.no_transcribe and all_new_videos:
        print(f"\nTranscribing {len(all_new_videos)} videos (tiered)...")
        ip_blocked = False

        for i, video in enumerate(all_new_videos):
            vid_id = video["video_id"]
            # Skip if already transcribed
            if os.path.exists(os.path.join(TRANSCRIPT_DIR, f"{vid_id}.json")):
                print(f"  [{i+1}/{len(all_new_videos)}] {vid_id} - already transcribed, skipping")
                video["has_transcript"] = True
                video["word_count"] = 0  # Will be read from file
                continue

            print(f"  [{i+1}/{len(all_new_videos)}] {video['title'][:50]}...", end=" ")
            time.sleep(TRANSCRIPT_DELAY)

            if ip_blocked:
                # Skip tier 1 if we got IP blocked, go straight to tier 2/3
                result = transcribe_tier2(vid_id)
                if "full_text" not in result or result.get("word_count", 0) == 0:
                    result = transcribe_tier3(vid_id)
            else:
                result = transcribe_video(vid_id)

            if result.get("error") == "ip_blocked":
                ip_blocked = True
                print("IP BLOCKED - switching to tier 2/3")
                result = transcribe_tier2(vid_id)
                if "full_text" not in result or result.get("word_count", 0) == 0:
                    result = transcribe_tier3(vid_id)

            tier = result.get("tier", 0)
            words = result.get("word_count", 0)

            if words > 0:
                save_transcript(vid_id, result, video)
                video["has_transcript"] = True
                video["word_count"] = words
                transcript_stats[f"tier{tier}"] = transcript_stats.get(f"tier{tier}", 0) + 1
                print(f"OK (tier {tier}, {words}w)")
            else:
                video["has_transcript"] = False
                video["word_count"] = 0
                transcript_stats["failed"] += 1
                print(f"FAILED ({result.get('error', 'unknown')[:40]})")

    # ---- X/TWITTER SCAN ----
    twitter_results = []
    if not args.no_twitter and not args.scan_only and not args.discovery_only and twitter_config.get("accounts"):
        print(f"\n{'=' * 60}")
        print("X/TWITTER SCAN (Apify)")
        print(f"{'=' * 60}")
        twitter_results = twitter_scan(twitter_config, discovery_cutoff, twitter_registry)
        save_json(TWITTER_REGISTRY_FILE, twitter_registry)
    elif args.twitter_only and twitter_config.get("accounts"):
        print(f"{'=' * 60}")
        print("X/TWITTER SCAN (Apify) - Twitter only mode")
        print(f"{'=' * 60}")
        twitter_results = twitter_scan(twitter_config, discovery_cutoff, twitter_registry)
        save_json(TWITTER_REGISTRY_FILE, twitter_registry)

    # ---- UPDATE REGISTRY ----
    for video in all_new_videos:
        vid_id = video["video_id"]
        registry["videos"][vid_id] = {
            "video_id": vid_id, "title": video.get("title", ""),
            "creator": video.get("creator", ""), "creator_handle": video.get("creator_handle", ""),
            "channel_id": video.get("channel_id", ""), "published": video.get("published", ""),
            "description": video.get("description", ""), "thumbnail": video.get("thumbnail", ""),
            "view_count": video.get("view_count"), "duration": video.get("duration"),
            "word_count": video.get("word_count", 0),
            "has_transcript": video.get("has_transcript", False),
            "source": video.get("source", ""), "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
    registry["last_scan"] = datetime.now(timezone.utc).isoformat()
    save_json(REGISTRY_FILE, registry)

    # ---- WRITE DIGEST ----
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    os.makedirs(DIGEST_DIR, exist_ok=True)

    digest_videos = []
    for v in all_new_videos:
        vid_id = v["video_id"]
        tp = os.path.join(TRANSCRIPT_DIR, f"{vid_id}.json")
        has_t = os.path.exists(tp)
        words = v.get("word_count", 0)
        if has_t and words == 0:
            try:
                td = json.load(open(tp))
                words = td.get("word_count", 0)
            except:
                pass

        digest_videos.append({
            "id": vid_id, "creator": v.get("creator", ""), "title": v.get("title", ""),
            "words": words, "published": v.get("published", ""),
            "view_count": v.get("view_count"),
            "transcript_path": f"skills/youtube-monitor/transcripts/{vid_id}.json" if has_t else None,
            "source": v.get("source", ""),
            "platform": "youtube",
        })

    # Add twitter results to digest
    for t in twitter_results:
        digest_videos.append({
            "id": t["id"], "creator": t.get("creator", ""), "title": t.get("title", ""),
            "text": t.get("text", ""), "words": t.get("words", 0),
            "published": t.get("published", ""),
            "view_count": t.get("view_count"),
            "likes": t.get("likes", 0), "retweets": t.get("retweets", 0),
            "replies": t.get("replies", 0), "url": t.get("url", ""),
            "transcript_path": None,
            "source": "twitter",
            "platform": "twitter",
        })

    digest_videos.sort(key=lambda x: x.get("view_count") or 0, reverse=True)

    yt_items = [v for v in digest_videos if v.get("platform") == "youtube"]
    tw_items = [v for v in digest_videos if v.get("platform") == "twitter"]

    digest = {
        "date": today, "pipeline_version": "v5-multiplatform",
        "platforms": ["youtube", "twitter"],
        "channels_scanned": len(channels_data.get("channels", [])),
        "discovery_terms": len(keywords_data.get("search_terms", [])),
        "total_items": len(digest_videos),
        "youtube_videos": len(yt_items),
        "twitter_tweets": len(tw_items),
        "channel_scan_videos": sum(1 for v in digest_videos if v["source"] == "channel_scan"),
        "discovery_videos": sum(1 for v in digest_videos if v["source"] == "discovery"),
        "with_transcripts": sum(1 for v in digest_videos if v.get("transcript_path")),
        "total_words": sum(v["words"] for v in digest_videos),
        "transcript_tiers": transcript_stats,
        "discovery_stats": discovery_stats,
        "registry_total": len(registry["videos"]),
        "twitter_registry_total": len(twitter_registry.get("tweet_ids", [])),
        "videos": digest_videos,
    }
    save_json(os.path.join(DIGEST_DIR, f"{today}.json"), digest)

    # ---- REPORT ----
    print(f"\n{'=' * 60}")
    print(f"PIPELINE COMPLETE")
    print(f"{'=' * 60}")
    print(f"YouTube - Channel scan: {digest['channel_scan_videos']}, Discovery: {digest['discovery_videos']}")
    print(f"Twitter - Tweets: {digest['twitter_tweets']}")
    print(f"Total items: {digest['total_items']}")
    print(f"Transcripts: {digest['with_transcripts']} ({digest['total_words']} words)")
    print(f"Tiers: T1={transcript_stats['tier1']} T2={transcript_stats['tier2']} T3={transcript_stats['tier3']} Failed={transcript_stats['failed']}")
    print(f"Digest: {DIGEST_DIR}/{today}.json")
    print(f"Registry: {len(registry['videos'])} videos, {len(twitter_registry.get('tweet_ids',[]))} tweets")

    # Fix 1: Zero-result alert — send bus message to nick2 immediately on failure.
    # Includes failure reason so nick2 can pick the right response.
    # This fires regardless of whether Apify key issues are resolved — permanent safety net.
    youtube_total = digest["youtube_videos"]
    discovery_count = digest["discovery_videos"]
    skipped_total = sum(v.get("skipped_existing", 0) for v in [digest])  # not a field, use registry delta

    alert_reasons = []
    if apify_channel_failed:
        alert_reasons.append("Apify channel actor unavailable (fell back to yt-dlp)")
    if discovery_error:
        zero_terms = discovery_stats.get("consecutive_zero_terms", "?")
        alert_reasons.append(f"yt-dlp discovery error: {zero_terms}+ consecutive zero-result search terms (possible rate limit)")
    if youtube_total == 0 and not apify_channel_failed and not discovery_error:
        # Pipeline ran fully but produced nothing — likely dedup bug (Mode 2)
        reg_total = len(registry["videos"])
        alert_reasons.append(f"Silent dedup failure: 0 videos despite registry_total={reg_total}. All new videos may be incorrectly flagged as skipped_existing.")

    if youtube_total == 0 or (discovery_count == 0 and not args.scan_only):
        reason_str = "; ".join(alert_reasons) if alert_reasons else "unknown — check pipeline logs"
        alert_msg = (
            f"data2 pipeline alert [{today}]: 0 {'total YouTube' if youtube_total == 0 else 'discovery'} videos. "
            f"Reason: {reason_str}. "
            f"Registry: {len(registry['videos'])} videos. "
            f"Manual queue available: append URLs to data2/.claude/skills/youtube-monitor/manual-queue.txt"
        )
        print(f"\nALERT: {alert_msg}", file=sys.stderr)
        # Send bus message to nick2
        try:
            agent_name = os.environ.get("CTX_AGENT_NAME", "data2")
            subprocess.run(
                ["cortextos", "bus", "send-message", "nick2", "urgent", alert_msg],
                timeout=10, capture_output=True,
            )
        except Exception as e:
            print(f"  WARN: could not send bus alert to nick2: {e}", file=sys.stderr)

    # Print JSON summary for cron consumption
    print(json.dumps({
        "status": "complete", "date": today,
        "total_items": digest["total_items"],
        "youtube_videos": digest["youtube_videos"],
        "twitter_tweets": digest["twitter_tweets"],
        "channel_scan": digest["channel_scan_videos"],
        "discovery": digest["discovery_videos"],
        "transcripts": digest["with_transcripts"],
        "words": digest["total_words"],
        "tiers": transcript_stats,
        "registry_total": len(registry["videos"]),
        "twitter_registry_total": len(twitter_registry.get("tweet_ids", [])),
        "apify_channel_failed": apify_channel_failed,
        "discovery_error": discovery_error,
    }, indent=2))


if __name__ == "__main__":
    sys.exit(main())
