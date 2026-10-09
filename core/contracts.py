# -*- coding: utf-8 -*-
"""
xhs-pipeline Data Contracts (v0.3.0)
Unified, type-safe schema strictly separating:
1. Immutable Content Facts (ContentFact)
2. Ephemeral Access Context & Query References (NoteReference)
3. Multi-keyword Discovery Records (DiscoveryRecord)
4. Time-series Engagement Snapshots (EngagementMetrics / EngagementSnapshot)
5. Traceable Analytical Insights (ContentInsight)
6. Task Execution State & Research Reproducibility Manifest (CrawlTaskManifest)
7. Explicit Data Completeness & Field Presence Semantics (FieldPresence, ContentCompleteness)
"""

import time
import re
import hashlib
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple, Union


# =====================================================================
# Domain Exceptions
# =====================================================================

class PipelineError(Exception):
    """Base exception for all xhs-pipeline errors."""
    pass


class DriverError(PipelineError):
    """Raised when an underlying scraper driver encounters a failure."""
    pass


class AuthenticationError(DriverError):
    """Raised when driver session has expired or requires user intervention."""
    pass


class RateLimitError(DriverError):
    """Raised when the platform WAF/anti-bot rate limit is triggered (HTTP 429)."""
    pass


class ReferenceExpiredError(DriverError):
    """Raised when an access token or query reference has expired and must be re-searched."""
    def __init__(self, message: str, note_id: str = "", needs_re_search: bool = True):
        super().__init__(message)
        self.note_id = note_id
        self.needs_re_search = needs_re_search


class ResourceNotFoundError(DriverError):
    """Raised when a specific note or creator cannot be located."""
    pass


class CapabilityUnsupportedError(DriverError):
    """Raised when a driver is invoked for an unsupported capability."""
    pass


class BudgetExceededError(PipelineError):
    """Raised when the requested operation exceeds the configured request budget."""
    pass


class CircuitBreakerOpenError(PipelineError):
    """Raised when a driver's circuit breaker is in OPEN state preventing dangerous calls."""
    pass


# =====================================================================
# Field Presence & Content Completeness Semantics
# =====================================================================

class FieldPresence(str, Enum):
    """
    Explicitly distinguishes valid data from various missing states,
    preventing adapters from fabricating dummy values (e.g. 0 for uncollected comments).
    """
    VALID = "valid"              # Fetched successfully with valid content
    NOT_FETCHED = "not_fetched"  # Not requested or not yet crawled (e.g. search snippet stage)
    FETCH_FAILED = "fetch_failed"# Attempted to fetch but encountered error
    UNSUPPORTED = "unsupported"  # Underlying platform/driver cannot provide this field
    KNOWN_EMPTY = "known_empty"  # Verified to be legitimately empty on platform


class ContentCompleteness(str, Enum):
    """Explicit declaration of note text completeness."""
    FULL = "full"            # Full text captured with no apparent truncation
    TRUNCATED = "truncated"  # Long text truncated by platform UI/API limit
    EMPTY = "empty"          # Note has zero description
    VIDEO_ONLY = "video_only"# Video post where narrative content is in audio/video rather than text


class StageStatus(str, Enum):
    """Lifecycle stage status for persistent state machine tracking."""
    PENDING = "pending"
    DISCOVERED = "discovered"
    CRAWLED = "crawled"
    ANALYZED = "analyzed"
    EXPORTED = "exported"
    FAILED = "failed"


@dataclass(frozen=True)
class DriverCapabilities:
    """Explicit behavioral declaration of what a scraper driver can execute."""
    can_search: bool = True
    can_get_detail: bool = True
    can_get_comments: bool = False
    can_get_media: bool = False
    requires_browser: bool = False
    supports_resume: bool = True


# =====================================================================
# Access Context & Query Reference (Separated from Business Entity)
# =====================================================================

@dataclass
class NoteReference:
    """
    Lightweight, driver-specific query reference returned during search/discovery.
    Encapsulates ephemeral access tokens (e.g., xsec_token) and browser session context.
    NEVER enters the knowledge base or permanent PKM entities.
    """
    note_id: str
    driver_name: str
    title_hint: str = ""
    author_hint: str = ""
    access_token: str = ""                       # Driver-specific token (e.g. xsec_token)
    access_context: Dict[str, Any] = field(default_factory=dict) # Cookies, URL signatures, headers
    discovered_keyword: str = ""
    rank: Optional[int] = None
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None           # Timestamp when access_token expires
    can_resume_cross_process: bool = True        # Can this token be reloaded across process restarts?
    is_stale: bool = False

    def is_expired(self) -> bool:
        if self.expires_at is not None and time.time() > self.expires_at:
            return True
        return self.is_stale


# =====================================================================
# Metric Snapshots & Discovery Records
# =====================================================================

def parse_metric_count(val: Any) -> Tuple[Optional[int], str, bool, FieldPresence]:
    """
    Parses Chinese social media metric strings into typed values and presence status.
    Examples:
        '8.7万' -> (87000, '8.7万', True, FieldPresence.VALID)
        '0'     -> (0, '0', False, FieldPresence.VALID)
        None    -> (None, '', False, FieldPresence.NOT_FETCHED)
    Returns:
        (parsed_integer_or_None, raw_string, is_approximate_flag, presence_status)
    """
    if val is None:
        return None, "", False, FieldPresence.NOT_FETCHED
    raw_str = str(val).strip()
    if not raw_str or raw_str in ("None", "null", "undefined"):
        return None, "", False, FieldPresence.NOT_FETCHED
    if raw_str in ("-1", "unsupported"):
        return None, "", False, FieldPresence.UNSUPPORTED

    is_approximate = False
    clean = raw_str.replace("+", "").strip()

    try:
        if "万" in clean:
            is_approximate = True
            num_part = clean.replace("万", "").strip()
            num = int(float(num_part) * 10000)
            return num, raw_str, is_approximate, FieldPresence.VALID
        elif "w" in clean.lower():
            is_approximate = True
            num_part = clean.lower().replace("w", "").strip()
            num = int(float(num_part) * 10000)
            return num, raw_str, is_approximate, FieldPresence.VALID
        elif clean.isdigit():
            return int(clean), raw_str, False, FieldPresence.VALID
        else:
            num = int(float(clean))
            return num, raw_str, False, FieldPresence.VALID
    except Exception:
        return None, raw_str, True, FieldPresence.FETCH_FAILED


@dataclass
class EngagementMetrics:
    """Structured metric snapshot distinguishing exact integers from approximate textual badges."""
    likes: Optional[int] = None
    raw_likes: str = ""
    likes_presence: FieldPresence = FieldPresence.NOT_FETCHED

    collects: Optional[int] = None
    raw_collects: str = ""
    collects_presence: FieldPresence = FieldPresence.NOT_FETCHED

    comments: Optional[int] = None
    raw_comments: str = ""
    comments_presence: FieldPresence = FieldPresence.NOT_FETCHED

    shares: Optional[int] = None
    raw_shares: str = ""
    shares_presence: FieldPresence = FieldPresence.NOT_FETCHED

    is_approximate: bool = False

    @classmethod
    def from_raw(
        cls,
        likes: Any = None,
        collects: Any = None,
        comments: Any = None,
        shares: Any = None
    ) -> "EngagementMetrics":
        l_num, l_raw, l_app, l_pres = parse_metric_count(likes)
        c_num, c_raw, c_app, c_pres = parse_metric_count(collects)
        cm_num, cm_raw, cm_app, cm_pres = parse_metric_count(comments)
        s_num, s_raw, s_app, s_pres = parse_metric_count(shares)

        return cls(
            likes=l_num, raw_likes=l_raw, likes_presence=l_pres,
            collects=c_num, raw_collects=c_raw, collects_presence=c_pres,
            comments=cm_num, raw_comments=cm_raw, comments_presence=cm_pres,
            shares=s_num, raw_shares=s_raw, shares_presence=s_pres,
            is_approximate=(l_app or c_app or cm_app or s_app)
        )


@dataclass
class EngagementSnapshot:
    """Time-series engagement snapshot allowing tracking of metric growth over time."""
    note_id: str
    metrics: EngagementMetrics
    collected_at: float = field(default_factory=time.time)


@dataclass
class DiscoveryRecord:
    """
    Independent record of a discovery pathway.
    Allows deduplicating the note entity while retaining complete multi-keyword trails.
    """
    source_type: str = "search_keyword"        # search_keyword, creator_feed, direct_url
    keyword: Optional[str] = None
    rank: Optional[int] = None
    discovered_at: float = field(default_factory=time.time)


# =====================================================================
# Content Fact (Immutable Content Representation)
# =====================================================================

@dataclass
class ContentFact:
    """
    Decoupled representation of immutable content facts.
    Separated from engagement metrics, access tokens, and research opinions.
    """
    note_id: str
    title: str
    desc: str
    note_type: str = "normal"                         # "normal" | "video"
    author_id: str = ""                               # Stable anonymized identifier
    author_name: str = ""                             # Display name
    completeness: ContentCompleteness = ContentCompleteness.FULL
    desc_presence: FieldPresence = FieldPresence.VALID
    media_presence: FieldPresence = FieldPresence.VALID
    tag_list: List[str] = field(default_factory=list)
    image_list: List[str] = field(default_factory=list)
    video_url: str = ""
    source_url: str = ""
    published_at: Optional[int] = None
    content_hash: str = ""

    def __post_init__(self):
        if not self.content_hash:
            raw_data = f"{self.title}|{self.desc}".encode("utf-8")
            self.content_hash = hashlib.sha256(raw_data).hexdigest()[:16]


# =====================================================================
# Analytical Inferences & Reproducibility Provenance
# =====================================================================

@dataclass
class ContentInsight:
    """
    Decoupled analytical inference with full provenance and reproducibility tracking.
    Never pollutes raw content facts in UnifiedNote or ContentFact.
    """
    insight_id: str
    note_id: str
    hook_archetype: str          # e.g., "emotional_contrast", "quantified_guide"
    key_themes: List[str] = field(default_factory=list)
    sentiment_tone: str = "neutral"
    evidence_snippets: List[str] = field(default_factory=list) # Exact verbatim quote from desc
    model_name: str = "empirical-miner-v1"                     # Model/analyzer identifier
    prompt_version: str = "2026.10"                            # Prompt version for reproducibility
    input_content_hash: str = ""                               # Content hash at time of analysis
    confidence_score: float = 1.0                              # Confidence level (0.0 to 1.0)
    recommendations: List[str] = field(default_factory=list)
    analyzed_at: float = field(default_factory=time.time)


# =====================================================================
# Core Unified Note Entity
# =====================================================================

@dataclass
class UnifiedNote:
    """
    Standardized, verified representation of a Xiaohongshu note.
    Free of driver-specific tokens, cookies, or internal handles.
    Cleanly composes ContentFact, EngagementMetrics, DiscoveryRecords, and ContentInsights.
    """
    note_id: str
    title: str = ""
    desc: str = ""
    note_type: str = "normal"                         # "normal" | "video"
    author_id: str = ""                               # Stable anonymized identifier
    author_name: str = ""                             # Display name
    metrics: EngagementMetrics = field(default_factory=lambda: EngagementMetrics.from_raw())
    url: str = ""
    completeness: ContentCompleteness = ContentCompleteness.FULL
    desc_presence: FieldPresence = FieldPresence.VALID
    media_presence: FieldPresence = FieldPresence.VALID
    tag_list: List[str] = field(default_factory=list)
    image_list: List[str] = field(default_factory=list)
    video_url: str = ""
    discovery_records: List[DiscoveryRecord] = field(default_factory=list)
    insights: List[ContentInsight] = field(default_factory=list)
    published_at: Optional[int] = None
    crawled_at: float = field(default_factory=time.time)
    stage_status: StageStatus = StageStatus.CRAWLED

    @property
    def primary_keyword(self) -> str:
        for d in self.discovery_records:
            if d.keyword:
                return d.keyword
        return ""

    @property
    def fact(self) -> ContentFact:
        """Returns the decoupled immutable ContentFact component."""
        return ContentFact(
            note_id=self.note_id,
            title=self.title,
            desc=self.desc,
            note_type=self.note_type,
            author_id=self.author_id,
            author_name=self.author_name,
            completeness=self.completeness,
            desc_presence=self.desc_presence,
            media_presence=self.media_presence,
            tag_list=list(self.tag_list),
            image_list=list(self.image_list),
            video_url=self.video_url,
            source_url=self.url,
            published_at=self.published_at
        )

    @property
    def content_hash(self) -> str:
        return self.fact.content_hash

    def add_discovery(self, keyword: Optional[str] = None, rank: Optional[int] = None, source_type: str = "search_keyword"):
        """Append a discovery pathway without overwriting previous associations."""
        for rec in self.discovery_records:
            if rec.keyword == keyword and rec.source_type == source_type:
                return
        self.discovery_records.append(DiscoveryRecord(source_type=source_type, keyword=keyword, rank=rank))

    def add_insight(self, insight: ContentInsight):
        """Attach a decoupled traceable analytical insight."""
        self.insights.append(insight)

    def get_sanitized_desc(self) -> str:
        """Sanitizes text to neutralize Markdown/HTML prompt injection attempts."""
        text = self.desc or ""
        text = text.replace("<script", "&lt;script").replace("</script>", "&lt;/script&gt;")
        text = text.replace("<!--", "&lt;!--").replace("-->", "--&gt;")
        # Prevent breaking YAML Frontmatter
        text = re.sub(r'^\s*---\s*$', ' - - - ', text, flags=re.MULTILINE)
        return text

    def get_sanitized_title(self) -> str:
        title = self.title or ""
        title = title.replace("\n", " ").replace("\r", " ").strip()
        return re.sub(r'[\\/*?:"<>|]', "", title)[:60]

    def to_dict(self) -> Dict[str, Any]:
        """Serializes note into a flattened dictionary suitable for CSV / DataFrame exports."""
        keywords_str = ",".join([d.keyword for d in self.discovery_records if d.keyword])
        return {
            "note_id": self.note_id,
            "title": self.title,
            "desc": self.desc,
            "completeness": self.completeness.value,
            "desc_presence": self.desc_presence.value,
            "note_type": self.note_type,
            "author_id": self.author_id,
            "author_name": self.author_name,
            "likes_num": self.metrics.likes,
            "likes_raw": self.metrics.raw_likes,
            "likes_presence": self.metrics.likes_presence.value,
            "collects_num": self.metrics.collects,
            "collects_raw": self.metrics.raw_collects,
            "collects_presence": self.metrics.collects_presence.value,
            "comments_num": self.metrics.comments,
            "comments_raw": self.metrics.raw_comments,
            "comments_presence": self.metrics.comments_presence.value,
            "shares_num": self.metrics.shares,
            "shares_raw": self.metrics.raw_shares,
            "shares_presence": self.metrics.shares_presence.value,
            "is_approximate_metric": self.metrics.is_approximate,
            "url": self.url,
            "tags": ",".join(self.tag_list),
            "images": ",".join(self.image_list),
            "video_url": self.video_url,
            "keywords": keywords_str,
            "published_at": self.published_at,
            "crawled_at": self.crawled_at,
            "stage_status": self.stage_status.value,
            "content_hash": self.content_hash,
            "insight_count": len(self.insights)
        }


# =====================================================================
# Research Reproducibility Manifest (Search Context)
# =====================================================================

@dataclass
class CrawlTaskManifest:
    """
    Complete audit trail and context of a search/scrape task.
    Preserves exact search criteria, quotas, filtering, and sample coverage
    for rigorous academic and empirical reproduction.
    """
    task_id: str
    driver_name: str
    driver_version: str
    pipeline_version: str
    started_at: float
    completed_at: Optional[float] = None
    keywords: List[str] = field(default_factory=list)
    sort_type: str = "popularity_descending"
    request_quota: int = 20
    actual_discovered_count: int = 0
    deduplicated_count: int = 0
    detail_success_count: int = 0
    detail_failed_count: int = 0
    stop_reason: str = "completed"              # "completed", "budget_exceeded", "cancelled", "rate_limited"
    filter_criteria: Dict[str, Any] = field(default_factory=dict)
    discovery_trails: List[Dict[str, Any]] = field(default_factory=list) # keyword -> rank -> note_id
    content_hashes: Dict[str, str] = field(default_factory=dict)         # note_id -> content_hash

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =====================================================================
# Comments Contract
# =====================================================================

@dataclass
class UnifiedComment:
    """Standardized comment structure."""
    comment_id: str
    note_id: str
    author_name: str
    content: str
    liked_count: int
    sub_comments_count: int = 0
    created_at: Optional[int] = None
    sub_comments: List[Dict[str, Any]] = field(default_factory=list)


# =====================================================================
# Pipeline Requests and Responses
# =====================================================================

@dataclass
class CrawlRequest:
    """Request payload sent to scraper drivers."""
    keywords: List[str]
    max_count_per_keyword: int = 20
    enable_comments: bool = False
    enable_media: bool = False
    sort_type: str = "popularity_descending"
    task_id: str = ""

    def __post_init__(self):
        if not self.task_id:
            self.task_id = f"task_{int(time.time()*1000)}"


@dataclass
class CrawlResponse:
    """Response returned by scraper drivers, supporting partial success semantics."""
    success: bool
    driver_name: str
    total_notes: int
    notes: List[UnifiedNote]
    references: List[NoteReference] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    output_files: Dict[str, str] = field(default_factory=dict)
    is_partial: bool = False
    manifest: Optional[CrawlTaskManifest] = None
