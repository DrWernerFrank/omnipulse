"""URL canonicalisation for stage-1 exact deduplication."""

from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_PARAMS = {
    "fbclid", "gclid", "dclid", "msclkid", "yclid", "mc_cid", "mc_eid", "_hsenc", "_hsmi", "mkt_tok",
    "ref_src", "ref_url", "cmpid", "ncid", "ocid", "taid", "guccounter", "guce_referrer",
    "guce_referrer_sig", "smid", "smtyp", "igshid", "spm", "sr_share", "at_medium", "at_campaign",
    "at_custom1", "at_custom2", "at_custom3", "at_custom4", "rss", "feedtype",
    "xtor", "__twitter_impression", "s_cid", "wt_mc", "dicbo", "itm_source", "itm_medium",
    "itm_campaign", "trk", "intcid", "utm",
}
_AMP_PATH = re.compile(r"(/amp/?$|\.amp$|/amp(?=/))")


def _is_tracking(key: str) -> bool:
    k = key.lower()
    return k.startswith(("utm_", "pk_", "hsa_", "mtm_")) or k in TRACKING_PARAMS


def strip_tracking(url: str) -> str:
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if not parts.query:
        return url
    q = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not _is_tracking(k)]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q), parts.fragment))


def canonicalize(url: str) -> str:
    """Normalise scheme/host, drop tracking params, fragments, AMP suffixes and trailing slashes."""
    url = url.strip()
    try:
        parts = urlsplit(url)
    except ValueError:
        return url.lower()
    host = (parts.hostname or "").lower()
    if host.startswith(("www.", "m.", "amp.", "mobile.")) and host.count(".") >= 2:
        host = host.split(".", 1)[1]
    if parts.port and parts.port not in (80, 443):
        host = f"{host}:{parts.port}"
    path = _AMP_PATH.sub("", parts.path) or "/"
    path = re.sub(r"/{2,}", "/", path)
    if len(path) > 1:
        path = path.rstrip("/")
    q = sorted((k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
               if not _is_tracking(k) and k.lower() not in ("amp", "outputtype"))
    return urlunsplit(("https", host, path, urlencode(q), ""))


def url_key(url: str) -> str:
    return hashlib.blake2b(canonicalize(url).encode(), digest_size=12).hexdigest()
