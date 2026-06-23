#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-cycle Atyrau media monitor.

The script is intentionally dependency-free so it can run from the bundled
Windows Python launcher without a virtual environment.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import email.utils
import gzip
import hashlib
import html
import json
import logging
import os
import re
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zlib
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


ROOT = Path(__file__).resolve().parent
STATE_DIR = ROOT / "state"
STATE_PATH = STATE_DIR / "monitor_state.json"
SENT_CANONICAL_PATH = STATE_DIR / "SENT_CANONICAL.txt"
LOG_PATH = STATE_DIR / "monitor.log"
ENV_PATH = ROOT / ".env"
LOCK_PATH = STATE_DIR / "monitor.lock"

UTC = timezone.utc
try:
    ATYRAU_TZ = ZoneInfo("Asia/Atyrau")
except ZoneInfoNotFoundError:
    ATYRAU_TZ = timezone(timedelta(hours=5), name="Asia/Atyrau")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36 AtyrauMonitor/1.0"
)
FETCH_TIMEOUT = 10
DISCOVERY_FETCH_TIMEOUT = 4
SEARCH_FETCH_TIMEOUT = 6
EXTERNAL_AGGREGATOR_FETCH_TIMEOUT = 3
SUPPLEMENTARY_SOURCE_TIME_BUDGET_SECONDS = 12
SUPPLEMENTARY_DISCOVERY_INTERVAL_MINUTES = 60
SUPPLEMENTARY_PRIORITY_INTERVAL_MINUTES = 30
SUPPLEMENTARY_BOOTSTRAP_WINDOW_MINUTES = 15
MAX_BYTES = 6 * 1024 * 1024
MAIN_LINK_LIMIT = 50
RECHECK_MAIN_TOP_N = 0
RECENT_UNSENT_RECHECK_TOP_N = 8
RECENT_UNSENT_RECHECK_MAX_AGE_MINUTES = 12 * 60
RECENT_UNSENT_RECHECK_INTERVAL_MINUTES = 5
PRIORITY_RECHECK_MAIN_TOP_N = 1
PRIORITY_RECHECK_INTERVAL_MINUTES = 60
TOP_RECHECK_SOURCE_HOSTS = {"azh.kz", "aikyn.kz", "atr.kz", "pricom.kz"}
SUPPLEMENTARY_PRIORITY_HOSTS = TOP_RECHECK_SOURCE_HOSTS
ARTICLE_WORKERS = 16
SOURCE_WORKERS = 24
SEARCH_WORKERS = 32
SUPPLEMENTARY_LINK_LIMIT = 50
MAX_SUPPLEMENTARY_CANDIDATES_PER_SOURCE = 10
MAX_SUPPLEMENTARY_CANDIDATES_PER_CYCLE = 80
SOURCE_INTERNAL_SEARCH_URL_LIMIT = 4
SUPPLEMENTARY_SITEMAP_CHILD_LIMIT = 2
SUPPLEMENTARY_CATEGORY_URL_LIMIT = 2
EXTERNAL_SEARCH_RESULTS_PER_QUERY = 5
MAX_EXTERNAL_SEARCH_CANDIDATES_PER_CYCLE = 50
MAX_EXTERNAL_AGGREGATOR_RESOLUTION_PER_CYCLE = 8
MAX_EXTERNAL_AGGREGATOR_RESOLUTION_PER_HOST = 2
EXTERNAL_SEARCH_RESULT_MAX_AGE_DAYS = 2
MIN_SEARCH_PROVIDERS_PER_QUERY = 2
MAX_SEARCH_PROVIDERS_PER_QUERY = 3
PRIORITY_SEARCH_PROVIDER_LIMIT = 4
YANDEX_QUERY_LIMIT_PER_CYCLE = 6
BING_WEB_QUERY_LIMIT_PER_CYCLE = 25
DISCOVERED_SOURCE_SAMPLE_LIMIT = 5
DISCOVERED_SOURCE_PROMOTE_FOUND_COUNT = 4
DISCOVERED_SOURCE_PROMOTE_RELEVANT_COUNT = 2
DISCOVERED_SOURCE_PROMOTE_SCORE = 10
MAX_SENT_ARTICLE_KEYS = 2000
INITIAL_LOOKBACK_MINUTES = 30
DELAY_FAILURE_MINUTES = 20
MAX_ALERT_DELAY_MINUTES = 600
FUTURE_DATE_TOLERANCE_MINUTES = 15
LOCK_STALE_MINUTES = 45
LEGACY_STATE_CLONE_DISABLED = {"Ак Жайык"}


SOURCES = [
    {"name": "NewTimes", "url": "https://newtimes.kz/novosti"},
    {"name": "Ак Жайык", "url": "https://azh.kz/ru/news/in-atyrau"},
    {"name": "Прикаспийская коммуна", "url": "https://pricom.kz/"},
    {"name": "ATR", "url": "https://atr.kz/"},
    {"name": "Inform.kz", "url": "https://www.inform.kz/lenta/"},
    {"name": "Kazinform", "url": "https://kaz.inform.kz/lenta/"},
    {"name": "Zakon.kz", "url": "https://www.zakon.kz/news/"},
    {"name": "Ulysmedia", "url": "https://ulysmedia.kz/news/"},
    {"name": "KazTAG", "url": "https://kaztag.kz/ru/news/"},
    {"name": "Курсив", "url": "https://kz.kursiv.media/"},
    {"name": "Orda", "url": "https://orda.kz/last-news/"},
    {"name": "Tengrinews", "url": "https://tengrinews.kz/news/"},
    {"name": "Inbusiness", "url": "https://inbusiness.kz/ru/lastnews"},
    {"name": "Informburo", "url": "https://informburo.kz/"},
    {"name": "КТК", "url": "https://www.ktk.kz/ru/news/"},
    {"name": "Время", "url": "https://time.kz/news"},
    {"name": "Aikyn", "url": "https://aikyn.kz/news"},
]

EXTERNAL_SEARCH_BASE_QUERIES = [
    "Атырау авария",
    "Атырау ДТП",
    "Атырау жол апаты",
    "Атырау пожар",
    "Атырау өрт",
    "Атырау взрыв",
    "Атырау жарылыс",
    "Атырау жалоба",
    "Атырау шағым",
    "Атырау шағым тіркелді",
    "Атырауда шағым тіркелді",
    "Атырау тұрғындар шағымданды",
    "Атырау халық наразы",
    "Атырау наразылық",
    "Атырау қарсылық",
    "Атырау работники жалоба",
    "Атырау жұмысшылар шағым",
    "Атырау зарплата",
    "Атырау жалақы",
    "Атырау жалақы дауы",
    "Атырау трудовой спор",
    "Атырау еңбек дауы",
    "Атырау отпуск",
    "Атырау еңбек демалысы",
    "Атырау трудовая инспекция",
    "Атырау еңбек инспекциясы",
    "Атырау экс-премьер",
    "Атырау экс-аким",
    "Атырау родственник чиновника",
    "Атырау племянник",
    "Атырау акционер",
    "Атырау прибыль",
    "санаторий Атырау",
    "Атырау экология",
    "Атырау выброс",
    "Атырау загрязнение",
    "Атырау ауа ластанды",
    "Атырау су ластанды",
    "Атырау отключение воды",
    "Атырау су жоқ",
    "Атырау жарық жоқ",
    "Атырау отключение света",
    "Атырау газ жоқ",
    "Атырау коррупция",
    "Атырау пара",
    "Атырау взятка",
    "Атырау уголовное дело",
    "Атырау қылмыстық іс",
    "Атырау задержали",
    "Атырау ұсталды",
    "Атырау суд",
    "Атырау сот",
    "Атырау полиция",
    "Атырау прокуратура",
    "Атырау акимат критика",
    "Атырау әкімдік сын",
    "Атырау ТШО",
    "Атырау Tengizchevroil",
    "Атырау NCOC",
    "Атырау АНПЗ",
    "Атырау KPI",
    "Атырау КазМунайГаз",
    "Тенгиз инцидент",
    "Тенгиз авария",
    "Тенгиз өндірістік апат",
    "Кульсары происшествие",
    "Құлсары оқиға",
    "Жылыой авария",
    "Жылыой оқиға",
    "Макат ДТП",
    "Мақат жол апаты",
    "Доссор пожар",
    "Доссор өрт",
    "Серик Шапкенов",
    "Серік Шәпкенов",
]

EXTERNAL_SEARCH_SETTLEMENTS = [
    "Кульсары",
    "Құлсары",
    "Жылыой",
    "Макат",
    "Мақат",
    "Доссор",
    "Индер",
    "Махамбет",
    "Курмангазы",
    "Құрманғазы",
    "Исатай",
    "Тенгиз",
    "Теңіз",
    "Карабатан",
    "Қарабатан",
    "Аккыстау",
    "Аққыстау",
]

EXTERNAL_SEARCH_SETTLEMENT_PROBLEMS = [
    "авария",
    "ДТП",
    "пожар",
    "жалоба",
    "отключение воды",
    "экология",
]

EXTERNAL_SEARCH_QUERIES = list(
    OrderedDict.fromkeys(
        EXTERNAL_SEARCH_BASE_QUERIES
        + [
            f"{settlement} {problem}"
            for settlement in EXTERNAL_SEARCH_SETTLEMENTS
            for problem in EXTERNAL_SEARCH_SETTLEMENT_PROBLEMS
        ]
    )
)

SEARCH_PROVIDER_URLS = [
    {
        "name": "Bing News RSS",
        "kind": "rss",
        "template": "https://www.bing.com/news/search?q={query}&format=rss&mkt=ru-RU&setlang=ru",
    },
    {
        "name": "Google News Search RSS RU recent",
        "kind": "rss",
        "template": "https://news.google.com/rss/search?q={query}+when%3A2d&hl=ru&gl=KZ&ceid=KZ:ru",
    },
    {
        "name": "Google News Search RSS KK recent",
        "kind": "rss",
        "template": "https://news.google.com/rss/search?q={query}+when%3A2d&hl=kk&gl=KZ&ceid=KZ:kk",
    },
    {
        "name": "Yandex News HTML",
        "kind": "html",
        "template": "https://yandex.kz/news/search?text={query}&rpt=nnews2&lr=162",
        "max_query_index": YANDEX_QUERY_LIMIT_PER_CYCLE,
        "force_query_index": YANDEX_QUERY_LIMIT_PER_CYCLE,
    },
    {
        "name": "Google News RSS KK",
        "kind": "rss",
        "template": "https://news.google.com/rss/search?q={query}&hl=kk&gl=KZ&ceid=KZ:kk",
    },
    {
        "name": "Google News RSS RU",
        "kind": "rss",
        "template": "https://news.google.com/rss/search?q={query}&hl=ru&gl=KZ&ceid=KZ:ru",
    },
    {
        "name": "Bing Web HTML",
        "kind": "html",
        "template": "https://www.bing.com/search?q={query}&mkt=ru-RU&setlang=ru&cc=KZ&count=10",
        "max_query_index": BING_WEB_QUERY_LIMIT_PER_CYCLE,
    },
    {
        "name": "Yandex Search HTML",
        "kind": "html",
        "template": "https://yandex.kz/search/?text={query}&lr=162",
        "max_query_index": YANDEX_QUERY_LIMIT_PER_CYCLE,
    },
]

EXTERNAL_SEARCH_ORIGIN = "external_search"
SEARCH_AGGREGATOR_HOSTS = {
    "bing.com",
    "google.com",
    "news.google.com",
    "yandex.kz",
    "yandex.ru",
    "yandex.com",
    "ya.ru",
    "dzen.ru",
    "news.mail.ru",
}
SEARCH_BLOCKED_RE = re.compile(
    r"(captcha|showcaptcha|smartcaptcha|are you a robot|robot check|"
    r"подтвердите, что вы не робот|подозрительный трафик|доступ ограничен)",
    re.I,
)
EXTERNAL_NO_DATE_STRONG_RISK_TERMS = {
    "жалоба",
    "жалобы",
    "жалуются",
    "возмущ",
    "критик",
    "коррупц",
    "взятк",
    "уголов",
    "задержан",
    "арест",
    "авари",
    "дтп",
    "пожар",
    "взрыв",
    "инцидент",
    "погиб",
    "смерт",
    "травм",
    "выброс",
    "загрязн",
    "запах",
    "смог",
    "сероводород",
    "гибель рыбы",
    "погибла рыба",
    "мертвая рыба",
    "гибель животных",
    "протест",
    "митинг",
    "конфликт",
    "наразыл",
    "қарсылық",
    "қарсылық білдір",
    "резонанс",
    "происшеств",
    "ЧП",
    "төтенше",
    "апат",
    "өрт",
    "жарылыс",
    "қаза",
    "ұсталды",
    "шағым",
    "ластан",
}


REGIONAL_TERMS = [
    "Атырау",
    "Атырауская область",
    "Атырауской области",
    "Атырауская обл",
    "Атырау облысы",
    "Atyrau",
    "Жайық",
    "Жайык",
    "Урал",
    "Каспий",
    "Каспийское море",
    "Құлсары",
    "Кульсары",
    "Жылыой",
    "Жылыойский",
    "Мақат",
    "Макат",
    "Доссор",
    "Индер",
    "Махамбет",
    "Құрманғазы",
    "Курмангазы",
    "Исатай",
    "Теңіз",
    "Тенгиз",
    "Қарабатан",
    "Карабатан",
    "Сарайшық",
    "Сарайшык",
    "Аккыстау",
    "Аққыстау",
    "Ганюшкино",
    "Дамба",
    "Еркинкала",
    "Балыкши",
    "Привокзальный",
    "Геолог",
    "Нурсая",
]
REGIONAL_STEM_TERMS = {
    "Атырау",
    "Атырауск",
    "Индер",
    "Индерск",
    "Жылыой",
    "Жылыойск",
    "Мақат",
    "Макат",
    "Макатск",
    "Доссор",
    "Доссорск",
    "Кульсар",
    "Құлсар",
    "Тенгиз",
    "Теңіз",
    "Карабатан",
    "Қарабатан",
    "Аккыстау",
    "Аққыстау",
    "Ганюшкин",
    "Дамба",
    "Еркинкал",
    "Балыкш",
    "Привокзальн",
    "Нурсай",
    "Махамбет",
    "Махамбетск",
    "Исатай",
    "Исатайск",
    "Курмангаз",
    "Құрманғаз",
    "Сарайш",
}

ORGANIZATION_TERMS = [
    "ТШО",
    "Тенгизшевройл",
    "Tengizchevroil",
    "TCO",
    "NCOC",
    "КазМунайГаз",
    "КМГ",
    "АНПЗ",
    "KPI",
    "акимат Атырауской области",
    "полиция",
    "ДП",
    "ДЧС",
    "ТЖД",
    "прокуратура",
    "суд",
]

GENERIC_ORGANIZATION_TERMS = {"полиция", "ДП", "ДЧС", "ТЖД", "прокуратура", "суд"}
WEAK_REGION_TERMS = {"Жайық", "Жайык", "Урал", "Каспий", "Каспийское море"}
AMBIGUOUS_REGION_TERMS = {
    "\u0422\u0435\u04a3\u0456\u0437",  # Теңіз: also means "sea" in Kazakh.
    "\u0421\u0430\u0440\u0430\u0439\u0448\u044b\u049b",  # Сарайшық
    "\u0421\u0430\u0440\u0430\u0439\u0448\u044b\u043a",  # Сарайшык
    "\u049a\u04b1\u0440\u043c\u0430\u043d\u0493\u0430\u0437\u044b",  # Құрманғазы
    "\u041a\u0443\u0440\u043c\u0430\u043d\u0433\u0430\u0437\u044b",  # Курмангазы
    "\u041c\u0430\u0445\u0430\u043c\u0431\u0435\u0442",  # Махамбет
    "\u0418\u0441\u0430\u0442\u0430\u0439",  # Исатай
}
REGION_ANCHOR_TERMS = {
    "\u0410\u0442\u044b\u0440\u0430\u0443",  # Атырау
    "\u0410\u0442\u044b\u0440\u0430\u0443\u0441\u043a\u0430\u044f \u043e\u0431\u043b\u0430\u0441\u0442\u044c",
    "\u0410\u0442\u044b\u0440\u0430\u0443\u0441\u043a\u043e\u0439 \u043e\u0431\u043b\u0430\u0441\u0442\u0438",
    "\u0410\u0442\u044b\u0440\u0430\u0443\u0441\u043a\u0430\u044f \u043e\u0431\u043b",
    "\u0410\u0442\u044b\u0440\u0430\u0443 \u043e\u0431\u043b\u044b\u0441\u044b",
    "Atyrau",
}
LOCAL_REGION_SOURCE_HOSTS = {"azh.kz", "pricom.kz", "atr.kz"}
AMBIGUOUS_REGION_LOCAL_CONTEXT = {
    "\u0430\u0443\u0434\u0430\u043d",
    "\u0440\u0430\u0439\u043e\u043d",
    "\u0430\u0443\u044b\u043b",
    "\u0441\u0435\u043b\u043e",
    "\u0441\u0435\u043b\u044c\u0441\u043a",
    "\u043c\u0435\u0441\u0442\u043e\u0440\u043e\u0436\u0434",
    "\u043a\u0435\u043d \u043e\u0440\u043d",
    "\u043f\u043e\u0441\u0435\u043b",
    "\u043a\u0435\u043d\u0442",
    "\u043e\u043a\u0440\u0443\u0433",
    "\u0442\u0448\u043e",
    "tco",
    "\u0442\u0435\u043d\u0433\u0438\u0437\u0448\u0435\u0432\u0440\u043e\u0439\u043b",
    "tengizchevroil",
    "\u0432\u0430\u0445\u0442",
}
AMBIGUOUS_REGION_STREET_CONTEXT = {
    "\u043a\u04e9\u0448",
    "\u0443\u043b\u0438\u0446",
    "\u0434\u0430\u04a3\u0493\u044b\u043b",
    "\u043f\u0440\u043e\u0441\u043f\u0435\u043a\u0442",
    "\u043f\u0430\u043c\u044f\u0442\u043d\u0438\u043a",
    "\u0435\u0441\u043a\u0435\u0440\u0442\u043a\u0456\u0448",
    "\u0430\u0442\u044b\u043d\u0434\u0430\u0493\u044b",
    "\u0438\u043c\u0435\u043d\u0438",
    "\u043c\u0438\u043a\u0440\u043e\u0440\u0430\u0439\u043e\u043d",
    "\u0436\u043a",
}
WEAK_REGION_RISK_TERMS = {
    "авари",
    "дтп",
    "пожар",
    "взрыв",
    "инцидент",
    "погиб",
    "смерт",
    "травм",
    "выброс",
    "загрязн",
    "запах",
    "смог",
    "сероводород",
    "гибель рыбы",
    "погибла рыба",
    "мертвая рыба",
    "гибель животных",
    "происшеств",
    "ЧП",
    "төтенше",
    "апат",
    "өрт",
    "жарылыс",
    "қаза",
    "ластан",
}
WEAK_REGION_ENVIRONMENT_RISK_TERMS = {
    "эколог",
    "выброс",
    "загрязн",
    "запах",
    "смог",
    "сероводород",
    "гибель рыбы",
    "погибла рыба",
    "мертвая рыба",
    "ластан",
}

RISK_TERMS = [
    "жалоба",
    "жалобы",
    "жалуются",
    "возмущ",
    "критик",
    "коррупц",
    "взятк",
    "уголов",
    "задержан",
    "арест",
    "суд",
    "приговор",
    "авари",
    "дтп",
    "жол апаты",
    "пожар",
    "взрыв",
    "эвакуац",
    "инцидент",
    "погиб",
    "пострада",
    "смерт",
    "өлтір",
    "травм",
    "отключен",
    "отключ",
    "без света",
    "без воды",
    "нет газа",
    "нет канализации",
    "нет дорог",
    "нет инфраструктуры",
    "нормальных дорог",
    "условий для жизни",
    "годами ждут",
    "инженерно-коммуникац",
    "газ жоқ",
    "су жоқ",
    "жарық жоқ",
    "коммуналь",
    "эколог",
    "выброс",
    "загрязн",
    "разлив",
    "запах",
    "смог",
    "сероводород",
    "гибель рыбы",
    "погибла рыба",
    "мертвая рыба",
    "гибель животных",
    "протест",
    "митинг",
    "наразы",
    "наразыл",
    "қарсылық",
    "қарсылық білдір",
    "конфликт",
    "расслед",
    "проверк",
    "резонанс",
    "происшеств",
    "ЧП",
    "төтенше",
    "апат",
    "өрт",
    "жарылыс",
    "қаза",
    "қылмыстық іс",
    "күдікті",
    "ұсталды",
    "сот",
    "сыбайлас жемқорлық",
    "шағым",
    "ластан",
]

ARTICLE_HINTS = [
    "/news/",
    "/novosti/",
    "/ru/news/",
    "/kazakhstan/",
    "/articles/",
    "/article/",
    "/lenta/",
    "/last-news/",
    "/ru/lastnews",
    "/ru/news",
    "/kaz/",
    "/ru/",
]

BAD_URL_PARTS = [
    "/tag/",
    "/tags/",
    "/category/",
    "/author/",
    "/authors/",
    "/search",
    "/page/",
    "/privacy",
    "/advert",
    "/about",
    "/contacts",
    "/rss",
    "/feed",
    "/sitemap",
    "mailto:",
    "tel:",
    "javascript:",
]

COMMON_FEED_PATHS = [
    "/rss",
    "/rss.xml",
    "/feed",
    "/feed/",
    "/atom.xml",
    "/ru/rss",
    "/ru/rss.xml",
    "/news/rss",
    "/news/rss.xml",
]

COMMON_SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemap-news.xml",
    "/news-sitemap.xml",
    "/sitemap_news.xml",
    "/ru/sitemap.xml",
]

VOID_HTML_TAGS = {
    "area",
    "base",
    "br",
    "col",
    "embed",
    "hr",
    "img",
    "input",
    "link",
    "meta",
    "param",
    "source",
    "track",
    "wbr",
}

CHROME_TAGS = {"nav", "footer", "header", "aside"}
CHROME_CLASS_RE = re.compile(
    r"(?:^|[\s_-])("
    r"nav|navbar|menu|footer|sidebar|breadcrumb|breadcrumbs|pagination|"
    r"comments?|related|social|advert|ads?|banner|subscribe|weather|forecast"
    r")(?:$|[\s_-])",
    re.I,
)
SITE_BOILERPLATE_CUTOFF_RE = re.compile(
    r"\s+(?:последние новости\s+)?(?:горячие объявления\s+)?"
    r"использование материалов сайта\b.*$",
    re.I | re.S,
)
ARTICLE_RELATED_CUTOFF_RE = re.compile(
    r"\s+(?:читайте также|оқыңыз|сондай-ақ оқыңыз|related|read also)\s*[:：].*$",
    re.I | re.S,
)

SEARCH_TERMS = [
    "Атырау",
    "Атырау облысы",
    "Atyrau",
    "Тенгиз",
    "Теңіз",
    "Кульсары",
    "Құлсары",
    "Жылыой",
    "Мақат",
    "Макат",
    "Индер",
    "Доссор",
]
SEARCH_PATTERNS = [
    "/search?q={q}",
    "/search?query={q}",
    "/ru/search?q={q}",
    "/ru/search?query={q}",
    "/?s={q}",
]


def now_utc() -> datetime:
    return datetime.now(UTC)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(UTC).isoformat()


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=ATYRAU_TZ).astimezone(UTC)
        return dt.astimezone(UTC)
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ATYRAU_TZ)
        return dt.astimezone(UTC)
    except Exception:
        pass
    for pattern in ("%d.%m.%Y %H:%M", "%d.%m.%Y, %H:%M", "%Y-%m-%d %H:%M:%S", "%d.%m.%Y"):
        try:
            return datetime.strptime(value[:19], pattern).replace(tzinfo=ATYRAU_TZ).astimezone(UTC)
        except ValueError:
            continue
    return None


def load_env(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    if path.exists():
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            result[key.strip()] = value.strip().strip('"').strip("'")
    for key in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        value = os.environ.get(key)
        if value:
            result[key] = value
    return result


def update_env_value(path: Path, key: str, value: str) -> None:
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines() if path.exists() else []
    replaced = False
    updated = []
    for line in lines:
        if line.strip().startswith("#") or "=" not in line:
            updated.append(line)
            continue
        existing_key, _existing_value = line.split("=", 1)
        if existing_key.strip() == key:
            updated.append(f"{key}={value}")
            replaced = True
        else:
            updated.append(line)
    if not replaced:
        updated.append(f"{key}={value}")
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text("\n".join(updated) + "\n", encoding="utf-8")
    tmp.replace(path)


def setup_logging() -> None:
    STATE_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        filename=str(LOG_PATH),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        encoding="utf-8",
    )


def load_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {
            "version": 1,
            "sources": {},
            "seen_links": {},
            "discovered_sources": {},
            "sent_article_keys": {},
            "last_significant_sent_at": None,
            "last_heartbeat_date": None,
            "last_successful_run_at": None,
        }
    try:
        with STATE_PATH.open("r", encoding="utf-8") as fh:
            state = json.load(fh)
        state.setdefault("sources", {})
        state.setdefault("seen_links", {})
        state.setdefault("discovered_sources", {})
        state.setdefault("sent_article_keys", {})
        return state
    except Exception:
        backup = STATE_PATH.with_suffix(f".broken-{int(time.time())}.json")
        STATE_PATH.replace(backup)
        logging.exception("State file was unreadable; moved to %s", backup)
        return load_state()


def save_state(state: dict[str, Any]) -> None:
    STATE_DIR.mkdir(exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2, sort_keys=True)
    tmp.replace(STATE_PATH)


def acquire_run_lock() -> bool:
    STATE_DIR.mkdir(exist_ok=True)
    try:
        age_seconds = time.time() - LOCK_PATH.stat().st_mtime
        if age_seconds > LOCK_STALE_MINUTES * 60:
            LOCK_PATH.unlink()
    except FileNotFoundError:
        pass

    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        fh.write(f"pid={os.getpid()} acquired_at={now_utc().isoformat()}\n")
    return True


def release_run_lock() -> None:
    try:
        LOCK_PATH.unlink()
    except FileNotFoundError:
        pass


def load_sent_canonicals() -> set[str]:
    if not SENT_CANONICAL_PATH.exists():
        return set()
    return {
        line.strip()
        for line in SENT_CANONICAL_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
        if line.strip()
    }


def append_sent_canonical(url: str) -> None:
    STATE_DIR.mkdir(exist_ok=True)
    with SENT_CANONICAL_PATH.open("a", encoding="utf-8") as fh:
        fh.write(url.strip() + "\n")


def normalize_space(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def strip_site_boilerplate(value: str | None) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    text = ARTICLE_RELATED_CUTOFF_RE.sub("", text)
    return normalize_space(SITE_BOILERPLATE_CUTOFF_RE.sub("", text))


WORD_CHAR = r"0-9A-Za-zА-Яа-яЁёӘәІіҢңҒғҮүҰұҚқӨөҺһ"
STEM_TERMS = {
    "возмущ",
    "критик",
    "коррупц",
    "взятк",
    "уголов",
    "задержан",
    "задерж",
    "пострада",
    "авари",
    "эвакуац",
    "погиб",
    "смерт",
    "травм",
    "отключен",
    "отключ",
    "коммуналь",
    "эколог",
    "выброс",
    "загрязн",
    "разлив",
    "происшеств",
    "расслед",
    "проверк",
    "чиновн",
    "төтенше",
    "ластан",
    "наразыл",
    "қарсылық білдір",
}
WEAK_RISK_TERMS = {"эколог"}
ADMIN_NEUTRAL_HEADLINE_RE = re.compile(
    r"(назначен|назначили|тағайындал|избрали|утвержден|утвердили|заседан|совещан|отчет|брифинг|послание|"
    r"қызметке|тағайындау|мәжіліс|отырыс|есеп)",
    re.I,
)
COURT_ONLY_TERMS = {"суд", "сот"}
NEUTRAL_OFFICIAL_ESCALATION_TERMS = {
    "погиб",
    "погибли",
    "пострадали",
    "пострада",
    "ДТП",
    "жол апаты",
    "пожар",
    "өрт",
    "взрыв",
    "жарылыс",
    "коррупц",
    "пара",
    "взятк",
    "задерж",
    "задержан",
    "ұсталды",
    "күдікті",
    "уголов",
    "қылмыстық іс",
    "протест",
    "наразыл",
    "қарсылық",
    "выброс",
    "загрязн",
    "разлив нефти",
}
VERY_STRONG_SIGNAL_TERMS = [
    "погиб",
    "погибли",
    "пострадали",
    "пострада",
    "өлтір",
    "қаза",
    "ДТП",
    "жол апаты",
    "авария",
    "авари",
    "пожар",
    "өрт",
    "взрыв",
    "жарылыс",
    "эвакуация",
    "эвакуац",
    "ЧС",
    "төтенше",
    "загрязнение",
    "загрязн",
    "выброс",
    "разлив нефти",
    "мұнай төг",
    "коррупция",
    "коррупц",
    "пара",
    "взятка",
    "взятк",
    "задержание",
    "задержан",
    "задерж",
    "ұсталды",
    "күдікті",
    "уголовное дело",
    "уголов",
    "қылмыстық іс",
    "протест",
    "митинг",
    "наразы",
    "наразыл",
    "қарсылық",
]
MEDIUM_SIGNAL_TERMS = [
    "жалобы жителей",
    "жалоба",
    "жалобы",
    "шағым",
    "жалуются",
    "отключение воды",
    "отключение света",
    "отключение газа",
    "отключен",
    "отключ",
    "без воды",
    "без света",
    "нет газа",
    "нет канализации",
    "нет дорог",
    "нет инфраструктуры",
    "нормальных дорог",
    "условий для жизни",
    "годами ждут",
    "инженерно-коммуникац",
    "газ жоқ",
    "су жоқ",
    "жарық жоқ",
    "конфликт",
    "критика",
    "критик",
    "расследование",
    "расслед",
    "проверка",
    "проверк",
    "резонанс",
    "инцидент",
    "происшеств",
    "апат",
]
WEAK_SIGNAL_TERMS = [
    "суд",
    "сот",
    "полиция",
    "прокуратура",
    "акимат",
    "әкімдік",
    "депутат",
    "чиновник",
    "чиновн",
]
RESONANT_FINANCIAL_PERSON_TERMS = [
    "экс-премьер",
    "экс-министр",
    "экс-аким",
    "племянник",
    "родственник",
    "аффилирован",
    "чиновник",
    "чиновн",
    "шенеунік",
    "туысы",
    "жиені",
]
RESONANT_FINANCIAL_MONEY_TERMS = [
    "прибыль",
    "выручка",
    "заработал",
    "заработала",
    "заработали",
    "доход",
    "акционер",
    "бенефициар",
    "владеет",
    "доля",
    "актив",
    "млн тенге",
    "миллион",
    "пайда",
    "табыс",
    "млн теңге",
]
RESONANT_FINANCIAL_STEM_TERMS = {
    "экс-премьер",
    "экс-министр",
    "экс-аким",
    "племянник",
    "родствен",
    "аффилир",
    "чиновн",
    "шенеун",
    "туыс",
    "жиен",
    "прибыл",
    "выручк",
    "заработ",
    "доход",
    "акционер",
    "бенефициар",
    "владе",
    "доля",
    "актив",
    "миллион",
    "пайда",
    "табыс",
}
SIGNAL_STEM_TERMS = STEM_TERMS | {
    "авари",
    "пострада",
    "эвакуац",
    "загрязн",
    "коррупц",
    "задерж",
    "отключ",
    "расслед",
    "проверк",
    "чиновн",
}
NEUTRAL_OFFICIAL_RE = re.compile(
    r"(назначен|назначили|тағайындал|представил|представили|поздрав|құттық|"
    r"совещан|заседан|отырыс|мәжіліс|конференц|форум|культурн|спорт|турнир|"
    r"чемпионат|отчет|отч[её]т|есеп|брифинг|обычн[а-я]+\s+сообщен|"
    r"принял участие|посетил|встретил|подписал(?:и)?\s+меморандум)",
    re.I,
)
HARD_RISK_TERMS = {
    "жалоба",
    "жалобы",
    "жалуются",
    "возмущ",
    "критик",
    "коррупц",
    "взятк",
    "уголов",
    "задержан",
    "арест",
    "авари",
    "дтп",
    "пожар",
    "взрыв",
    "инцидент",
    "погиб",
    "смерт",
    "өлтір",
    "травм",
    "отключен",
    "без света",
    "без воды",
    "нет газа",
    "нет канализации",
    "нет дорог",
    "нет инфраструктуры",
    "нормальных дорог",
    "условий для жизни",
    "годами ждут",
    "инженерно-коммуникац",
    "коммуналь",
    "выброс",
    "загрязн",
    "запах",
    "смог",
    "сероводород",
    "гибель рыбы",
    "погибла рыба",
    "мертвая рыба",
    "гибель животных",
    "протест",
    "митинг",
    "наразыл",
    "қарсылық",
    "қарсылық білдір",
    "конфликт",
    "резонанс",
    "происшеств",
    "ЧП",
    "төтенше",
    "апат",
    "өрт",
    "жарылыс",
    "қаза",
    "қылмыстық іс",
    "күдікті",
    "ұсталды",
    "шағым",
    "ластан",
}
SKIP_HEADLINE_RE = re.compile(
    r"(календар[ья]|дни рождения|гороскоп|астропрогноз|погода|валют|курс валют|"
    r"контакты|contacts?|кинолектор|фестиваль|конкурс|выставк|спектакл|концерт)",
    re.I,
)
AD_URL_RE = re.compile(
    r"/(?:advert|advertising|promo|special-project|specialprojects|specprojects|sponsored|partner-material|brandvoice)(?:/|$|-)",
    re.I,
)
AD_LABEL_RE = re.compile(
    r"(?:"
    r"на правах рекламы|"
    r"рекламн(?:ый|ая|ое|ые)\s+материал|"
    r"партн[её]рск(?:ий|ая|ое)\s+материал|"
    r"спонсорск(?:ий|ая|ое)\s+материал|"
    r"при поддержке\s+[^.\n]{2,80}|"
    r"advertorial|sponsored content|paid content|"
    r"жарнамалық материал|демеушілік материал|"
    r"промо-?материал|pr-?материал"
    r")",
    re.I,
)
STATIC_NON_ARTICLE_RE = re.compile(
    r"/(?:content/)?(?:oferta|terms|rules|agreement|user-agreement|privacy|contacts?|kontakty|kontakt|"
    r"about|important_information|referat|essay|diplom|kursov|grafik|dashboard|air-quality|weather)(?:/|$|-)",
    re.I,
)
LOW_VALUE_EXTERNAL_HOSTS = {
    "stud.kz",
    "aqi.in",
    "iqair.com",
    "aqicn.org",
    "airkaz.org",
    "creditbureau.kz",
}
LOW_VALUE_EXTERNAL_PATH_RE = re.compile(
    r"/(?:referat|essay|diplom|kursov|kontakty|kontakt|contacts?|grafik|dashboard|air-quality|map)(?:/|$|-)",
    re.I,
)


def term_matches(text: str, term: str, allow_stem: bool = False) -> bool:
    escaped = re.escape(term.casefold())
    if allow_stem:
        pattern = rf"(?<![{WORD_CHAR}]){escaped}[{WORD_CHAR}]*"
    else:
        pattern = rf"(?<![{WORD_CHAR}]){escaped}(?![{WORD_CHAR}])"
    return re.search(pattern, text.casefold()) is not None


def text_contains_any(text: str, terms: Iterable[str], stem_terms: set[str] | None = None) -> list[str]:
    stem_terms = stem_terms or set()
    found = []
    for term in terms:
        allow_stem = term in stem_terms
        if term_matches(text, term, allow_stem=allow_stem):
            found.append(term)
    return list(OrderedDict.fromkeys(found))


def text_has_near_context(
    text: str,
    term: str,
    context_terms: set[str],
    window_chars: int = 80,
    allow_stem: bool = False,
) -> bool:
    folded = text.casefold()
    escaped = re.escape(term.casefold())
    if allow_stem:
        pattern = rf"(?<![{WORD_CHAR}]){escaped}[{WORD_CHAR}]*"
    else:
        pattern = rf"(?<![{WORD_CHAR}]){escaped}(?![{WORD_CHAR}])"
    for match in re.finditer(pattern, folded):
        start = max(0, match.start() - window_chars)
        end = min(len(folded), match.end() + window_chars)
        window = folded[start:end]
        if any(context.casefold() in window for context in context_terms):
            return True
    return False


def is_local_region_source(article: dict[str, Any]) -> bool:
    url = article.get("canonical") or article.get("url") or ""
    return url_host(str(url)) in LOCAL_REGION_SOURCE_HOSTS


def refine_region_matches(article: dict[str, Any], matches: list[str]) -> list[str]:
    if not matches:
        return []
    if is_local_region_source(article) or any(term in matches for term in REGION_ANCHOR_TERMS):
        return matches

    text = article.get("combinedText", "")
    refined = []
    for term in matches:
        if term not in AMBIGUOUS_REGION_TERMS:
            refined.append(term)
            continue
        allow_stem = term in REGIONAL_STEM_TERMS
        if text_has_near_context(text, term, AMBIGUOUS_REGION_STREET_CONTEXT, allow_stem=allow_stem):
            continue
        if text_has_near_context(text, term, AMBIGUOUS_REGION_LOCAL_CONTEXT, allow_stem=allow_stem):
            refined.append(term)
    return list(OrderedDict.fromkeys(refined))


def normalize_for_dedupe(value: str | None) -> str:
    text = normalize_space(value).casefold()
    if not text:
        return ""
    text = re.sub(rf"[^{WORD_CHAR}\s]+", " ", text)
    return normalize_space(text)


def article_title_key(article: dict[str, Any]) -> str:
    return normalize_for_dedupe(str(article.get("headline") or ""))


def article_date_key(article: dict[str, Any], run_at: datetime) -> str:
    published = article_published_at(article, run_at)
    if not published:
        return ""
    return published.astimezone(ATYRAU_TZ).date().isoformat()


def article_content_hash(article: dict[str, Any]) -> str:
    text = normalize_for_dedupe(
        str(article.get("articleBody") or article.get("combinedText") or article.get("description") or "")
    )
    if len(text) < 80:
        return ""
    return hashlib.sha1(text[:2500].encode("utf-8", errors="ignore")).hexdigest()


def article_signature(article: dict[str, Any], canonical: str, run_at: datetime) -> dict[str, str]:
    return {
        "canonical": canonical,
        "title_key": article_title_key(article),
        "date_key": article_date_key(article, run_at),
        "content_hash": article_content_hash(article),
    }


def duplicate_article_reason(
    state: dict[str, Any],
    article: dict[str, Any],
    canonical: str,
    run_at: datetime,
    sent_canonicals: set[str],
) -> str | None:
    if canonical in sent_canonicals:
        return "canonical"
    signature = article_signature(article, canonical, run_at)
    sent_keys = state.setdefault("sent_article_keys", {})
    title_key = signature["title_key"]
    date_key = signature["date_key"]
    content_hash = signature["content_hash"]
    for info in sent_keys.values():
        if not isinstance(info, dict):
            continue
        if content_hash and content_hash == info.get("content_hash"):
            return "content"
        same_title = title_key and title_key == info.get("title_key")
        same_date = not date_key or not info.get("date_key") or date_key == info.get("date_key")
        if same_title and same_date:
            return "title_date"
    return None


def remember_sent_article(
    state: dict[str, Any],
    article: dict[str, Any],
    analysis: dict[str, Any],
    canonical: str,
    run_at: datetime,
) -> None:
    sent_keys = state.setdefault("sent_article_keys", {})
    signature = article_signature(article, canonical, run_at)
    key = canonical or signature["content_hash"] or signature["title_key"]
    if not key:
        return
    sent_keys[key] = {
        **signature,
        "headline": normalize_space(str(article.get("headline") or ""))[:240],
        "source": normalize_space(str(article.get("source") or ""))[:120],
        "importance_level": str(analysis.get("importance_level") or ""),
        "importance_score": int(analysis.get("importance_score") or 0),
        "sent_at": iso(run_at),
    }
    if len(sent_keys) > MAX_SENT_ARTICLE_KEYS:
        ordered = sorted(
            sent_keys.items(),
            key=lambda item: str(item[1].get("sent_at") or "") if isinstance(item[1], dict) else "",
        )
        for old_key, _info in ordered[: len(sent_keys) - MAX_SENT_ARTICLE_KEYS]:
            sent_keys.pop(old_key, None)


def canonicalize_url(url: str, base: str | None = None) -> str:
    if base:
        url = urllib.parse.urljoin(base, url)
    url = html.unescape(url.strip())
    parsed = urllib.parse.urlsplit(url)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    filtered = [
        (k, v)
        for k, v in query_pairs
        if not k.lower().startswith("utm_")
        and k.lower()
        not in {
            "fbclid",
            "gclid",
            "yclid",
            "mc_cid",
            "mc_eid",
            "spm",
            "ref",
            "ref_src",
            "source",
            "share",
            "si",
            "ved",
            "usg",
            "from",
            "output",
            "amp",
            "feature",
        }
    ]
    query = urllib.parse.urlencode(filtered, doseq=True)
    return urllib.parse.urlunsplit((scheme, netloc, path, query, ""))


def source_state_key(source: dict[str, str]) -> str:
    return source.get("key") or canonicalize_url(source["url"])


def source_state_for(state: dict[str, Any], source: dict[str, str]) -> dict[str, Any]:
    sources_state = state.setdefault("sources", {})
    key = source_state_key(source)
    existing = sources_state.get(key)
    if isinstance(existing, dict):
        return existing

    source_name = source.get("name", "")
    legacy = sources_state.get(source_name)
    if (
        source_name not in LEGACY_STATE_CLONE_DISABLED
        and isinstance(legacy, dict)
        and (legacy.get("last_success_at") or legacy.get("last_links"))
    ):
        sources_state[key] = dict(legacy)
    else:
        sources_state[key] = {}
    return sources_state[key]


def same_domain(url: str, source_url: str) -> bool:
    u = urllib.parse.urlsplit(url)
    s = urllib.parse.urlsplit(source_url)
    return u.netloc.lower().removeprefix("www.") == s.netloc.lower().removeprefix("www.")


def url_host(url: str) -> str:
    return urllib.parse.urlsplit(url).netloc.lower().removeprefix("www.")


def is_main_source_domain(url: str) -> bool:
    return any(same_domain(url, source["url"]) for source in SOURCES)


def source_name_from_url(url: str) -> str:
    host = url_host(url)
    return host or "внешний сайт"


def source_root_url_from_host(host: str) -> str:
    host = host.strip().lower().removeprefix("www.")
    return f"https://{host}/" if host else ""


def monitor_sources_for_state(state: dict[str, Any]) -> list[dict[str, str]]:
    sources = [dict(source) for source in SOURCES]
    seen_hosts = {url_host(source["url"]) for source in sources}
    discovered = state.setdefault("discovered_sources", {})
    for host, info in sorted(discovered.items()):
        if not isinstance(info, dict) or not info.get("promoted"):
            continue
        host = str(info.get("domain") or host).lower().removeprefix("www.")
        if not host or host in seen_hosts or host in SEARCH_AGGREGATOR_HOSTS:
            continue
        source_url = str(info.get("source_url") or source_root_url_from_host(host))
        if not source_url:
            continue
        sources.append({"name": host, "url": source_url, "key": f"discovered:{host}"})
        seen_hosts.add(host)
    return sources


def is_monitored_source_domain(url: str, sources: Iterable[dict[str, str]]) -> bool:
    return any(same_domain(url, source["url"]) for source in sources)


def is_known_listing_url(url: str) -> bool:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc.lower().removeprefix("www.")
    path = parsed.path.rstrip("/").casefold()
    if host == "azh.kz" and re.fullmatch(r"/(?:ru|kz)/news(?:/[a-z0-9_-]+)?", path):
        return True
    return False


def is_known_non_article_url(url: str) -> bool:
    path = urllib.parse.urlsplit(url).path
    return is_known_listing_url(url) or STATIC_NON_ARTICLE_RE.search(path) is not None


def is_low_value_external_url(url: str) -> bool:
    host = url_host(url)
    path = urllib.parse.urlsplit(url).path
    return host in LOW_VALUE_EXTERNAL_HOSTS or LOW_VALUE_EXTERNAL_PATH_RE.search(path) is not None


def looks_like_article(url: str, anchor_text: str = "") -> bool:
    if is_known_non_article_url(url):
        return False
    lower = url.casefold()
    if any(part in lower for part in BAD_URL_PARTS):
        return False
    path = urllib.parse.urlsplit(url).path
    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg|css|js|ico|mp4|mp3|avi|zip|rar)$", path, re.I):
        return False
    if re.search(r"/\d{4}/\d{2}/\d{2}/", path):
        return True
    if re.search(r"/\d{4}-\d{2}-\d{2}/", path):
        return True
    if re.search(r"[-_/](\d{5,})(?:[-_/]|$)", path):
        return True
    if any(hint in lower for hint in ARTICLE_HINTS) and len(anchor_text.strip()) >= 8:
        return True
    if len(anchor_text.strip()) >= 25 and path.count("/") >= 2:
        return True
    return False


def looks_like_external_search_result(url: str, title: str = "") -> bool:
    if is_known_non_article_url(url) or is_low_value_external_url(url):
        return False
    host = url_host(url)
    if not host or host in SEARCH_AGGREGATOR_HOSTS:
        return False
    if looks_like_article(url, title):
        return True
    path = urllib.parse.urlsplit(url).path
    if path in {"", "/"}:
        return False
    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg|css|js|ico|mp4|mp3|avi|zip|rar)$", path, re.I):
        return False
    return len(normalize_space(title)) >= 20 and len(path.strip("/")) >= 8


def fetch_url(
    url: str,
    accept: str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    timeout: float = FETCH_TIMEOUT,
) -> tuple[str, str, str]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": accept,
            "Accept-Language": "ru-RU,ru;q=0.9,kk;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        final_url = response.geturl()
        content_type = response.headers.get("Content-Type", "")
        content_encoding = response.headers.get("Content-Encoding", "").casefold()
        raw = response.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raw = raw[:MAX_BYTES]
        if "gzip" in content_encoding or raw.startswith(b"\x1f\x8b"):
            raw = gzip.decompress(raw)
        elif "deflate" in content_encoding:
            try:
                raw = zlib.decompress(raw)
            except zlib.error:
                raw = zlib.decompress(raw, -zlib.MAX_WBITS)
        charset = response.headers.get_content_charset() or "utf-8"
        try:
            text = raw.decode(charset, errors="replace")
        except LookupError:
            text = raw.decode("utf-8", errors="replace")
        return final_url, content_type, text


class PageParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links: list[tuple[str, str, dict[str, str]]] = []
        self.meta: dict[str, list[str]] = {}
        self.link_rels: list[dict[str, str]] = []
        self.json_ld: list[str] = []
        self.text_parts: list[str] = []
        self.semantic_text_parts: list[str] = []
        self.content_text_parts: list[str] = []
        self.captions: list[str] = []
        self.title_parts: list[str] = []
        self.h1_parts: list[str] = []
        self.current_link: dict[str, Any] | None = None
        self.current_tag_stack: list[str] = []
        self.content_marker_stack: list[bool] = []
        self.chrome_marker_stack: list[bool] = []
        self.capture_jsonld = False
        self.skip_depth = 0
        self.content_depth = 0
        self.chrome_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {k.lower(): (v or "") for k, v in attrs}
        class_id = f"{attrs_dict.get('class', '')} {attrs_dict.get('id', '')}".casefold()
        content_marker = bool(tag in {"article", "main"} or re.search(
            r"(article|post|publication|material|story|entry|detail|content|body|text)",
            class_id,
        ))
        chrome_marker = bool(tag in CHROME_TAGS or CHROME_CLASS_RE.search(class_id))
        if tag not in VOID_HTML_TAGS:
            self.current_tag_stack.append(tag)
            self.content_marker_stack.append(content_marker)
            self.chrome_marker_stack.append(chrome_marker)
            if content_marker:
                self.content_depth += 1
            if chrome_marker:
                self.chrome_depth += 1
        if tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
        if tag == "script" and "ld+json" in attrs_dict.get("type", "").lower():
            self.capture_jsonld = True
            self.skip_depth = max(0, self.skip_depth - 1)
        if tag == "meta":
            key = (
                attrs_dict.get("name")
                or attrs_dict.get("property")
                or attrs_dict.get("itemprop")
                or attrs_dict.get("http-equiv")
            )
            content = attrs_dict.get("content")
            if key and content:
                self.meta.setdefault(key.lower(), []).append(content)
        if tag == "link":
            self.link_rels.append(attrs_dict)
        if tag == "a" and attrs_dict.get("href"):
            href = canonicalize_url(attrs_dict["href"], self.base_url)
            self.current_link = {"href": href, "text": [], "attrs": attrs_dict}

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in VOID_HTML_TAGS:
            self.handle_endtag(tag)

    def _pop_tag(self, tag: str) -> None:
        if tag not in self.current_tag_stack:
            return
        while self.current_tag_stack:
            current = self.current_tag_stack.pop()
            marker = self.content_marker_stack.pop() if self.content_marker_stack else False
            chrome_marker = self.chrome_marker_stack.pop() if self.chrome_marker_stack else False
            if marker and self.content_depth:
                self.content_depth -= 1
            if chrome_marker and self.chrome_depth:
                self.chrome_depth -= 1
            if current == tag:
                break

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.current_link:
            text = normalize_space(" ".join(self.current_link["text"]))
            self.links.append((self.current_link["href"], text, self.current_link["attrs"]))
            self.current_link = None
        if tag == "script" and self.capture_jsonld:
            self.capture_jsonld = False
        elif tag in {"script", "style", "noscript"} and self.skip_depth:
            self.skip_depth -= 1
        self._pop_tag(tag)

    def handle_data(self, data: str) -> None:
        text = normalize_space(data)
        if not text:
            return
        if self.capture_jsonld:
            self.json_ld.append(data)
            return
        if self.skip_depth:
            return
        if self.current_link is not None:
            self.current_link["text"].append(text)
        tag = self.current_tag_stack[-1] if self.current_tag_stack else ""
        if tag == "title":
            self.title_parts.append(text)
        elif tag == "h1":
            self.h1_parts.append(text)
        elif tag in {"figcaption", "caption"}:
            self.captions.append(text)
        if self.chrome_depth:
            return
        if tag in {"p", "h1", "h2", "h3", "blockquote", "li", "figcaption"}:
            self.semantic_text_parts.append(text)
        if self.content_depth:
            self.content_text_parts.append(text)
        if tag not in CHROME_TAGS:
            self.text_parts.append(text)


def parse_page(html_text: str, base_url: str) -> PageParser:
    parser = PageParser(base_url)
    try:
        parser.feed(html_text)
    except Exception:
        logging.debug("HTML parser failed for %s", base_url, exc_info=True)
    return parser


def recursive_json_values(obj: Any, keys: set[str]) -> list[Any]:
    values = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in keys:
                values.append(value)
            values.extend(recursive_json_values(value, keys))
    elif isinstance(obj, list):
        for item in obj:
            values.extend(recursive_json_values(item, keys))
    return values


def parse_json_ld(raw_blocks: Iterable[str]) -> list[Any]:
    results = []
    for raw in raw_blocks:
        text = raw.strip()
        if not text:
            continue
        try:
            results.append(json.loads(text))
            continue
        except json.JSONDecodeError:
            pass
        for match in re.finditer(r"\{.*?\}", text, flags=re.S):
            try:
                results.append(json.loads(match.group(0)))
            except json.JSONDecodeError:
                continue
    return results


def first_meta(parser: PageParser, *keys: str) -> str:
    for key in keys:
        values = parser.meta.get(key.lower())
        if values:
            return normalize_space(values[0])
    return ""


def canonical_from_parser(parser: PageParser, final_url: str) -> str:
    for attrs in parser.link_rels:
        rel = attrs.get("rel", "").casefold()
        if "canonical" in rel and attrs.get("href"):
            return canonicalize_url(attrs["href"], final_url)
    og_url = first_meta(parser, "og:url")
    if og_url:
        return canonicalize_url(og_url, final_url)
    return canonicalize_url(final_url)


def extract_article(final_url: str, html_text: str, source_name: str) -> dict[str, Any]:
    parser = parse_page(html_text, final_url)
    json_ld = parse_json_ld(parser.json_ld)

    def first_json(keys: set[str]) -> str:
        for obj in json_ld:
            for value in recursive_json_values(obj, keys):
                if isinstance(value, str) and normalize_space(value):
                    return normalize_space(value)
                if isinstance(value, dict):
                    name = value.get("name")
                    if isinstance(name, str):
                        return normalize_space(name)
        return ""

    headline = (
        first_json({"headline"})
        or first_meta(parser, "og:title", "twitter:title")
        or normalize_space(" ".join(parser.h1_parts))
        or normalize_space(" ".join(parser.title_parts))
    )
    description = (
        first_json({"description"})
        or first_meta(parser, "description", "og:description", "twitter:description")
    )
    keywords = first_meta(parser, "keywords", "news_keywords")
    json_body = first_json({"articleBody"})
    visible_text = normalize_space(" ".join(parser.text_parts))
    content_text = normalize_space(" ".join(parser.content_text_parts))
    semantic_text = normalize_space(" ".join(parser.semantic_text_parts))
    if json_body:
        body_text = json_body
    elif len(semantic_text) >= 120:
        body_text = semantic_text
    else:
        body_text = content_text or semantic_text or visible_text
    body_text = strip_site_boilerplate(body_text)
    semantic_text = strip_site_boilerplate(semantic_text)
    content_text = strip_site_boilerplate(content_text)
    captions = normalize_space(" ".join(parser.captions))
    canonical = canonical_from_parser(parser, final_url)

    date_candidates = []
    date_candidates.extend(recursive_json_values(json_ld, {"datePublished", "dateCreated", "uploadDate"}))
    date_candidates.append(first_meta(
        parser,
        "article:published_time",
        "pubdate",
        "date",
        "publishdate",
        "datepublished",
    ))
    date_candidates.extend(re.findall(r'"datePublished"\s*:\s*"([^"]+)"', html_text, flags=re.I))
    date_candidates.extend(
        re.findall(r"(?<!\d)(\d{1,2}\.\d{1,2}\.\d{4}\s+\d{1,2}:\d{2})(?!\d)", html_text)
    )
    date_candidates.extend(
        re.findall(r"(?<!\d)(\d{1,2}\.\d{1,2}\.\d{4})(?!\d)", html_text)
    )
    date_published = None
    for candidate in date_candidates:
        if isinstance(candidate, str):
            date_published = parse_iso(candidate)
            if date_published:
                break

    modified_candidates = []
    modified_candidates.extend(recursive_json_values(json_ld, {"dateModified", "modified"}))
    modified_candidates.append(first_meta(parser, "article:modified_time", "lastmod"))
    date_modified = None
    for candidate in modified_candidates:
        if isinstance(candidate, str):
            date_modified = parse_iso(candidate)
            if date_modified:
                break

    author = first_json({"author"}) or first_meta(parser, "author", "article:author")
    tags = []
    tags.extend(parser.meta.get("article:tag", []))
    if keywords:
        tags.extend([part.strip() for part in re.split(r"[,;]", keywords) if part.strip()])
    embedded_docs = [
        href
        for href, _text, _attrs in parser.links
        if re.search(r"\.(pdf|doc|docx|xls|xlsx|ppt|pptx)(?:$|\?)", href, re.I)
    ]

    combined = "\n".join(
        part
        for part in [
            headline,
            description,
            keywords,
            normalize_space(" ".join(tags)),
            json_body,
            semantic_text,
            content_text if not semantic_text else "",
            body_text if not semantic_text and not content_text else "",
            captions,
            author,
            " ".join(embedded_docs),
        ]
        if part
    )
    combined = strip_site_boilerplate(combined)

    return {
        "source": source_name,
        "url": final_url,
        "canonical": canonical,
        "headline": headline or canonical,
        "description": description,
        "keywords": keywords,
        "tags": list(OrderedDict.fromkeys(tags)),
        "articleBody": body_text,
        "captions": captions,
        "author": author,
        "datePublished": iso(date_published),
        "dateModified": iso(date_modified),
        "embeddedDocs": embedded_docs,
        "combinedText": combined,
    }


def extract_links_from_html(html_text: str, base_url: str, source_url: str, limit: int = MAIN_LINK_LIMIT) -> list[str]:
    parser = parse_page(html_text, base_url)
    result: OrderedDict[str, None] = OrderedDict()
    for href, text, _attrs in parser.links:
        if not same_domain(href, source_url):
            continue
        normalized = canonicalize_url(href)
        if looks_like_article(normalized, text):
            result[normalized] = None
        if len(result) >= limit:
            break
    return list(result.keys())


def discover_feed_urls(html_text: str, final_url: str, source_url: str) -> list[str]:
    parser = parse_page(html_text, final_url)
    result: OrderedDict[str, None] = OrderedDict()
    for attrs in parser.link_rels:
        typ = attrs.get("type", "").casefold()
        rel = attrs.get("rel", "").casefold()
        href = attrs.get("href")
        if href and ("rss" in typ or "atom" in typ or "alternate" in rel):
            url = canonicalize_url(href, final_url)
            if same_domain(url, source_url):
                result[url] = None
    root = f"{urllib.parse.urlsplit(source_url).scheme}://{urllib.parse.urlsplit(source_url).netloc}"
    for path in COMMON_FEED_PATHS:
        result[canonicalize_url(path, root)] = None
    return list(result.keys())


def discover_sitemap_urls(source_url: str) -> list[str]:
    root = f"{urllib.parse.urlsplit(source_url).scheme}://{urllib.parse.urlsplit(source_url).netloc}"
    return [canonicalize_url(path, root) for path in COMMON_SITEMAP_PATHS]


def parse_xml_urls(xml_text: str, base_url: str, max_urls: int = 120) -> list[tuple[str, datetime | None]]:
    urls: list[tuple[str, datetime | None]] = []
    try:
        xml_text = re.sub(r"^\s*<\?xml[^>]*\?>", "", xml_text, count=1, flags=re.I)
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return urls

    def local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].lower()

    root_tag = local(root.tag)
    if root_tag in {"rss", "feed"}:
        for elem in root.iter():
            name = local(elem.tag)
            if name in {"item", "entry"}:
                link = ""
                published = None
                for child in elem:
                    cname = local(child.tag)
                    if cname == "link":
                        link = child.attrib.get("href") or (child.text or "")
                    elif cname in {"guid", "id"} and not link:
                        text = child.text or ""
                        if text.startswith("http"):
                            link = text
                    elif cname in {"pubdate", "published", "updated"}:
                        published = parse_iso(child.text)
                if link:
                    urls.append((canonicalize_url(link, base_url), published))
                    if len(urls) >= max_urls:
                        return urls
    elif root_tag == "sitemapindex":
        for sitemap_elem in root.iter():
            if local(sitemap_elem.tag) != "sitemap":
                continue
            loc = ""
            published = None
            for child in sitemap_elem:
                cname = local(child.tag)
                if cname == "loc":
                    loc = child.text or ""
                elif cname == "lastmod":
                    published = parse_iso(child.text)
            if loc:
                urls.append((canonicalize_url(loc, base_url), published))
                if len(urls) >= max_urls:
                    return urls
    else:
        for url_elem in root.iter():
            if local(url_elem.tag) != "url":
                continue
            loc = ""
            published = None
            for child in url_elem.iter():
                cname = local(child.tag)
                if cname == "loc":
                    loc = child.text or ""
                elif cname in {"lastmod", "publication_date"}:
                    published = parse_iso(child.text)
            if loc:
                urls.append((canonicalize_url(loc, base_url), published))
                if len(urls) >= max_urls:
                    return urls
    return urls


def unwrap_search_result_url(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc.lower().removeprefix("www.")
    if host == "bing.com" and parsed.path.lower().endswith("/news/apiclick.aspx"):
        target = urllib.parse.parse_qs(parsed.query).get("url", [""])[0]
        if target.startswith("http"):
            return canonicalize_url(target)
    if host == "bing.com" and parsed.path.lower().startswith("/ck/"):
        target = urllib.parse.parse_qs(parsed.query).get("u", [""])[0]
        target = urllib.parse.unquote(target)
        if target.startswith("http"):
            return canonicalize_url(target)
        if target.startswith("a1"):
            payload = target[2:]
            try:
                padded = payload + "=" * (-len(payload) % 4)
                decoded = base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
                if decoded.startswith("http"):
                    return canonicalize_url(decoded)
            except Exception:
                pass
    for param in ("url", "u", "q"):
        target = urllib.parse.parse_qs(parsed.query).get(param, [""])[0]
        target = urllib.parse.unquote(target)
        if target.startswith("http"):
            return canonicalize_url(target)
    return canonicalize_url(url)


def is_search_aggregator_url(url: str) -> bool:
    return url_host(url) in SEARCH_AGGREGATOR_HOSTS


def is_resolvable_search_aggregator_url(url: str) -> bool:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.netloc.lower().removeprefix("www.")
    path = parsed.path.rstrip("/").casefold()
    if host == "news.google.com" and (
        path.startswith("/rss/articles/")
        or path.startswith("/articles/")
        or path.startswith("/read/")
    ):
        return True
    return False


def clean_search_title(title: str) -> str:
    title = normalize_space(title)
    if " - " in title:
        title = title.rsplit(" - ", 1)[0]
    if " – " in title:
        title = title.rsplit(" – ", 1)[0]
    return normalize_space(title.strip("\"'“”«»"))


def title_token_set(title: str) -> set[str]:
    return {
        token
        for token in re.findall(rf"{WORD_CHAR}+", normalize_for_dedupe(title))
        if len(token) >= 3
    }


def title_match_score(expected_title: str, candidate_title: str) -> float:
    expected_key = normalize_for_dedupe(clean_search_title(expected_title))
    candidate_key = normalize_for_dedupe(candidate_title)
    if not expected_key or not candidate_key:
        return 0.0
    if expected_key in candidate_key or candidate_key in expected_key:
        return 1.0
    expected_tokens = title_token_set(expected_key)
    candidate_tokens = title_token_set(candidate_key)
    if not expected_tokens or not candidate_tokens:
        return 0.0
    overlap = len(expected_tokens & candidate_tokens)
    return overlap / max(1, min(len(expected_tokens), len(candidate_tokens)))


def promising_aggregator_result(result: ExternalSearchResult) -> bool:
    title = clean_search_title(result.title)
    if len(title) < 20:
        return False
    region_matches = text_contains_any(title, REGIONAL_TERMS, REGIONAL_STEM_TERMS)
    signal_matches = text_contains_any(
        title,
        VERY_STRONG_SIGNAL_TERMS + MEDIUM_SIGNAL_TERMS,
        SIGNAL_STEM_TERMS,
    )
    return bool(region_matches and signal_matches)


def resolve_external_from_source_homepage(result: ExternalSearchResult) -> str | None:
    source_url = canonicalize_url(result.source_url or "")
    if not source_url.startswith(("http://", "https://")):
        return None
    if is_search_aggregator_url(source_url) or is_low_value_external_url(source_url):
        return None
    try:
        final_url, _ct, html_text = fetch_url(source_url, timeout=EXTERNAL_AGGREGATOR_FETCH_TIMEOUT)
    except Exception:
        logging.debug("External source homepage lookup failed: %s", source_url, exc_info=True)
        return None

    parser = parse_page(html_text, final_url)
    best_url = ""
    best_score = 0.0
    for href, text, _attrs in parser.links:
        url = unwrap_search_result_url(canonicalize_url(href, final_url))
        if not same_domain(url, source_url):
            continue
        if is_known_non_article_url(url) or is_low_value_external_url(url):
            continue
        if not looks_like_external_search_result(url, text):
            continue
        score = title_match_score(result.title, text)
        if score > best_score:
            best_score = score
            best_url = url
        if best_score >= 0.92:
            break
    if best_url and best_score >= 0.55:
        return canonicalize_url(best_url)
    return None


def extract_urls_from_html_fragment(fragment: str, base_url: str) -> list[str]:
    urls: OrderedDict[str, None] = OrderedDict()
    for match in re.finditer(r"""href=["']([^"']+)["']""", fragment, flags=re.I):
        url = unwrap_search_result_url(canonicalize_url(match.group(1), base_url))
        if url.startswith(("http://", "https://")):
            urls[url] = None
    for match in re.finditer(r"https?://[^\s\"'<>]+", html.unescape(fragment)):
        url = unwrap_search_result_url(canonicalize_url(match.group(0), base_url))
        if url.startswith(("http://", "https://")):
            urls[url] = None
    return list(urls.keys())


def parse_search_feed_items(xml_text: str, base_url: str, max_items: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    try:
        xml_text = re.sub(r"^\s*<\?xml[^>]*\?>", "", xml_text, count=1, flags=re.I)
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items

    def local(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].lower()

    for elem in root.iter():
        if local(elem.tag) not in {"item", "entry"}:
            continue
        title = ""
        link = ""
        description = ""
        published = None
        source_url = ""
        for child in elem:
            cname = local(child.tag)
            if cname == "title":
                title = normalize_space(child.text or "")
            elif cname == "link":
                link = child.attrib.get("href") or (child.text or "")
            elif cname == "description":
                description = child.text or ""
            elif cname == "source":
                source_url = child.attrib.get("url", "")
            elif cname in {"pubdate", "published", "updated"}:
                published = parse_iso(child.text)
        candidate_urls: OrderedDict[str, None] = OrderedDict()
        if link:
            candidate_urls[unwrap_search_result_url(canonicalize_url(link, base_url))] = None
        for url in extract_urls_from_html_fragment(description, base_url):
            candidate_urls[url] = None
        for candidate_url in candidate_urls.keys():
            items.append(
                {
                    "url": candidate_url,
                    "title": title,
                    "published": published,
                    "source_url": canonicalize_url(source_url, base_url) if source_url else "",
                }
            )
            if len(items) >= max_items:
                return items
    return items


def parse_search_html_items(html_text: str, base_url: str, max_items: int) -> list[dict[str, Any]]:
    parser = parse_page(html_text, base_url)
    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for href, text, attrs in parser.links:
        if len(items) >= max_items:
            break
        url = unwrap_search_result_url(canonicalize_url(href, base_url))
        if url in seen_urls or is_search_aggregator_url(url):
            continue
        css = " ".join([attrs.get("class", ""), attrs.get("id", "")]).casefold()
        if "b_algo" not in css and not looks_like_external_search_result(url, text):
            continue
        if not looks_like_external_search_result(url, text):
            continue
        seen_urls.add(url)
        items.append({"url": url, "title": normalize_space(text), "published": None})
    return items


def extract_primary_url_from_aggregator(html_text: str, final_url: str) -> str | None:
    parser = parse_page(html_text, final_url)
    for href, text, _attrs in parser.links:
        url = unwrap_search_result_url(canonicalize_url(href, final_url))
        if is_search_aggregator_url(url):
            continue
        if looks_like_external_search_result(url, text):
            return url
    return None


def resolve_redirect_url(url: str) -> str:
    url = unwrap_search_result_url(url)
    if not url.startswith(("http://", "https://")):
        return canonicalize_url(url)
    try:
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,kk;q=0.8,en;q=0.7",
            },
            method="HEAD",
        )
        with urllib.request.urlopen(request, timeout=SEARCH_FETCH_TIMEOUT) as response:
            return unwrap_search_result_url(canonicalize_url(response.geturl()))
    except Exception:
        return canonicalize_url(url)


def normalize_external_result(result: ExternalSearchResult) -> ExternalSearchResult | None:
    initial_url = unwrap_search_result_url(result.url)
    url = initial_url if is_search_aggregator_url(initial_url) else resolve_redirect_url(initial_url)
    if is_search_aggregator_url(url):
        primary_url = resolve_external_from_source_homepage(result)
        if primary_url:
            url = resolve_redirect_url(primary_url)
        elif is_resolvable_search_aggregator_url(url):
            return None
        else:
            try:
                final_url, content_type, html_text = fetch_url(url, timeout=SEARCH_FETCH_TIMEOUT)
                primary_url = extract_primary_url_from_aggregator(html_text, final_url)
                if primary_url:
                    url = resolve_redirect_url(primary_url)
            except Exception:
                logging.debug("Aggregator extraction failed: %s", url, exc_info=True)
    if is_search_aggregator_url(url):
        return None
    if is_low_value_external_url(url):
        return None
    return ExternalSearchResult(
        url=canonicalize_url(url),
        query=result.query,
        provider=result.provider,
        title=result.title,
        published=result.published,
        source_url=result.source_url,
    )


def looks_like_sitemap_url(url: str) -> bool:
    path = urllib.parse.urlsplit(url).path.casefold()
    return path.endswith(".xml") or path.endswith(".xml.gz") or "sitemap" in path


def discover_category_urls(html_text: str, final_url: str, source_url: str) -> list[str]:
    parser = parse_page(html_text, final_url)
    result: OrderedDict[str, None] = OrderedDict()
    category_needles = [
        "атырау",
        "atyrau",
        "происше",
        "общество",
        "казахстан",
        "новости",
        "news",
        "tag",
        "category",
        "region",
    ]
    for href, text, _attrs in parser.links:
        combined = f"{href} {text}".casefold()
        if same_domain(href, source_url) and any(needle in combined for needle in category_needles):
            if not looks_like_article(href, text):
                result[href] = None
        if len(result) >= 6:
            break
    return list(result.keys())


def build_search_urls(source_url: str) -> list[str]:
    root = f"{urllib.parse.urlsplit(source_url).scheme}://{urllib.parse.urlsplit(source_url).netloc}"
    result = []
    for term in SEARCH_TERMS:
        q = urllib.parse.quote(term)
        for pattern in SEARCH_PATTERNS:
            result.append(canonicalize_url(pattern.format(q=q), root))
    return result[:SOURCE_INTERNAL_SEARCH_URL_LIMIT]


def fetch_wordpress_api_links(source_url: str, limit: int = MAIN_LINK_LIMIT) -> list[str]:
    root = f"{urllib.parse.urlsplit(source_url).scheme}://{urllib.parse.urlsplit(source_url).netloc}"
    api_url = f"{root}/wp-json/wp/v2/posts?per_page={limit}&_fields=link,date_gmt,modified_gmt,title,excerpt"
    try:
        final_url, _ct, text = fetch_url(api_url, "application/json,*/*;q=0.5", timeout=DISCOVERY_FETCH_TIMEOUT)
        payload = json.loads(text)
    except Exception:
        logging.debug("WordPress API failed: %s", api_url, exc_info=True)
        return []
    result: OrderedDict[str, None] = OrderedDict()
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            link = item.get("link")
            if isinstance(link, str):
                normalized = canonicalize_url(link, final_url)
                if same_domain(normalized, source_url) and looks_like_article(normalized, "wordpress api result"):
                    result[normalized] = None
    return list(result.keys())[:limit]


def discover_api_urls(html_text: str, source_url: str) -> list[str]:
    result: OrderedDict[str, None] = OrderedDict()
    for match in re.finditer(r"""["']([^"']*(?:/api/|/ajax/|/xhr/)[^"']*)["']""", html_text, flags=re.I):
        url = canonicalize_url(match.group(1), source_url)
        if same_domain(url, source_url):
            result[url] = None
        if len(result) >= 4:
            break
    return list(result.keys())


@dataclass
class SourceFetch:
    source: dict[str, str]
    ok: bool
    final_url: str = ""
    html_text: str = ""
    main_links: list[str] = field(default_factory=list)
    supplementary_links: list[str] = field(default_factory=list)
    error: str = ""


@dataclass
class ExternalSearchResult:
    url: str
    query: str
    provider: str
    title: str = ""
    published: datetime | None = None
    source_url: str = ""


def fetch_source_main(source: dict[str, str]) -> SourceFetch:
    try:
        final_url, content_type, html_text = fetch_url(source["url"])
        main_links = extract_links_from_html(html_text, final_url, source["url"], MAIN_LINK_LIMIT)
        return SourceFetch(source=source, ok=True, final_url=final_url, html_text=html_text, main_links=main_links)
    except Exception as exc:
        logging.debug("Main source fetch failed: %s %s", source["name"], exc, exc_info=True)
        return SourceFetch(
            source=source,
            ok=False,
            error=f"main source fetch failed: {type(exc).__name__}: {exc}",
        )


def fetch_source_supplementary(fetched: SourceFetch) -> SourceFetch:
    if not fetched.ok:
        return fetched
    source = fetched.source
    supplementary: OrderedDict[str, None] = OrderedDict()
    deadline = time.monotonic() + SUPPLEMENTARY_SOURCE_TIME_BUDGET_SECONDS

    def budget_exhausted() -> bool:
        return time.monotonic() >= deadline

    def timeout_left() -> float:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return 0.2
        return max(0.2, min(float(DISCOVERY_FETCH_TIMEOUT), remaining))

    try:
        if len(fetched.main_links) < MAIN_LINK_LIMIT and not budget_exhausted():
            for link in fetch_wordpress_api_links(source["url"], MAIN_LINK_LIMIT):
                if link not in fetched.main_links:
                    supplementary[link] = None
                if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                    break

        for url in discover_feed_urls(fetched.html_text, fetched.final_url, source["url"]):
            if budget_exhausted():
                break
            try:
                feed_final, _ct, feed_text = fetch_url(
                    url,
                    "application/rss+xml,application/atom+xml,application/xml,text/xml;q=0.9,*/*;q=0.5",
                    timeout=timeout_left(),
                )
                for link, published in parse_xml_urls(feed_text, feed_final):
                    if same_domain(link, source["url"]):
                        if not published or published >= now_utc() - timedelta(days=3):
                            supplementary[link] = None
                    if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                        break
            except Exception:
                logging.debug("Feed failed: %s", url, exc_info=True)
            if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                break

        for url in discover_sitemap_urls(source["url"]):
            if budget_exhausted():
                break
            try:
                sm_final, _ct, sm_text = fetch_url(
                    url,
                    "application/xml,text/xml;q=0.9,*/*;q=0.5",
                    timeout=timeout_left(),
                )
                parsed_urls = parse_xml_urls(sm_text, sm_final, SUPPLEMENTARY_LINK_LIMIT)
                if parsed_urls and any(looks_like_sitemap_url(link) for link, _p in parsed_urls[:10]):
                    for child, _published in parsed_urls[:SUPPLEMENTARY_SITEMAP_CHILD_LIMIT]:
                        if budget_exhausted():
                            break
                        try:
                            child_final, _ct, child_text = fetch_url(
                                child,
                                "application/xml,text/xml;q=0.9,*/*;q=0.5",
                                timeout=timeout_left(),
                            )
                            for link, published in parse_xml_urls(child_text, child_final):
                                if same_domain(link, source["url"]):
                                    if not published or published >= now_utc() - timedelta(days=3):
                                        supplementary[link] = None
                                if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                                    break
                        except Exception:
                            logging.debug("Child sitemap failed: %s", child, exc_info=True)
                else:
                    for link, published in parsed_urls:
                        if same_domain(link, source["url"]):
                            if not published or published >= now_utc() - timedelta(days=3):
                                supplementary[link] = None
                        if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                            break
            except Exception:
                logging.debug("Sitemap failed: %s", url, exc_info=True)
            if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                break

        for url in discover_category_urls(fetched.html_text, fetched.final_url, source["url"])[:SUPPLEMENTARY_CATEGORY_URL_LIMIT]:
            if budget_exhausted():
                break
            try:
                cat_final, _ct, cat_text = fetch_url(url, timeout=timeout_left())
                for link in extract_links_from_html(cat_text, cat_final, source["url"], 20):
                    supplementary[link] = None
            except Exception:
                logging.debug("Category failed: %s", url, exc_info=True)
            if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                break

        for url in build_search_urls(source["url"]):
            if budget_exhausted():
                break
            try:
                search_final, _ct, search_text = fetch_url(url, timeout=timeout_left())
                for link in extract_links_from_html(search_text, search_final, source["url"], 25):
                    supplementary[link] = None
            except Exception:
                logging.debug("Search failed: %s", url, exc_info=True)
            if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                break

        for url in discover_api_urls(fetched.html_text, source["url"]):
            if budget_exhausted():
                break
            try:
                api_final, _ct, api_text = fetch_url(
                    url,
                    "application/json,text/html,*/*",
                    timeout=timeout_left(),
                )
                for match in re.finditer(r"https?://[^\"'\\\s<>]+", api_text):
                    link = canonicalize_url(match.group(0))
                    if same_domain(link, source["url"]) and looks_like_article(link, "api result"):
                        supplementary[link] = None
            except Exception:
                logging.debug("API failed: %s", url, exc_info=True)
            if len(supplementary) >= SUPPLEMENTARY_LINK_LIMIT:
                break

        supplementary_links = [
            link
            for link in supplementary.keys()
            if looks_like_article(link, "supplementary") and link not in fetched.main_links
        ]
        fetched.supplementary_links = supplementary_links[:SUPPLEMENTARY_LINK_LIMIT]
        return fetched
    except Exception as exc:
        logging.debug("Supplementary discovery failed: %s %s", source["name"], exc, exc_info=True)
        fetched.error = f"supplementary discovery failed: {type(exc).__name__}: {exc}"
        return fetched


def fetch_source(source: dict[str, str]) -> SourceFetch:
    return fetch_source_supplementary(fetch_source_main(source))


def is_search_response_blocked(provider: dict[str, Any], final_url: str, text: str) -> bool:
    provider_name = str(provider.get("name") or "")
    if "yandex" not in provider_name.casefold() and "yandex" not in final_url.casefold():
        return False
    return bool(SEARCH_BLOCKED_RE.search(f"{final_url}\n{text[:2000]}"))


def fetch_external_search_query(query: str, query_index: int = 0) -> list[ExternalSearchResult]:
    results: OrderedDict[str, ExternalSearchResult] = OrderedDict()
    encoded_query = urllib.parse.quote_plus(query)
    cutoff = now_utc() - timedelta(days=EXTERNAL_SEARCH_RESULT_MAX_AGE_DAYS)
    providers_attempted = 0
    provider_limit = (
        PRIORITY_SEARCH_PROVIDER_LIMIT
        if query_index < len(EXTERNAL_SEARCH_BASE_QUERIES)
        else MAX_SEARCH_PROVIDERS_PER_QUERY
    )
    for provider_index, provider in enumerate(SEARCH_PROVIDER_URLS):
        max_query_index = provider.get("max_query_index")
        if isinstance(max_query_index, int) and query_index >= max_query_index:
            continue
        remaining_forced = any(
            isinstance(candidate.get("force_query_index"), int)
            and query_index < int(candidate["force_query_index"])
            for candidate in SEARCH_PROVIDER_URLS[provider_index:]
        )
        if (
            len(results) >= EXTERNAL_SEARCH_RESULTS_PER_QUERY
            and providers_attempted >= MIN_SEARCH_PROVIDERS_PER_QUERY
            and not remaining_forced
        ):
            break
        if providers_attempted >= provider_limit and not remaining_forced:
            break
        url = provider["template"].format(query=encoded_query)
        try:
            providers_attempted += 1
            kind = provider.get("kind", "rss")
            if kind == "html":
                final_url, _ct, search_text = fetch_url(
                    url,
                    "text/html,application/xhtml+xml,*/*;q=0.8",
                    timeout=SEARCH_FETCH_TIMEOUT,
                )
                if is_search_response_blocked(provider, final_url, search_text):
                    logging.warning("External search provider blocked: provider=%s query=%s", provider["name"], query)
                    continue
                items = parse_search_html_items(search_text, final_url, EXTERNAL_SEARCH_RESULTS_PER_QUERY)
            else:
                final_url, _ct, feed_text = fetch_url(
                    url,
                    "application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.5",
                    timeout=SEARCH_FETCH_TIMEOUT,
                )
                if is_search_response_blocked(provider, final_url, feed_text):
                    logging.warning("External search provider blocked: provider=%s query=%s", provider["name"], query)
                    continue
                items = parse_search_feed_items(feed_text, final_url, EXTERNAL_SEARCH_RESULTS_PER_QUERY)
            for item in items:
                if len(results) >= EXTERNAL_SEARCH_RESULTS_PER_QUERY:
                    break
                result_url = canonicalize_url(item.get("url") or "")
                if not result_url.startswith(("http://", "https://")):
                    continue
                resolvable_aggregator = is_resolvable_search_aggregator_url(result_url)
                if is_search_aggregator_url(result_url) and not resolvable_aggregator:
                    continue
                raw_source_url = str(item.get("source_url") or "").strip()
                source_url = canonicalize_url(raw_source_url) if raw_source_url else ""
                if is_main_source_domain(source_url or result_url):
                    continue
                title = str(item.get("title") or "")
                if resolvable_aggregator:
                    if len(clean_search_title(title)) < 20:
                        continue
                    if source_url and (
                        is_search_aggregator_url(source_url)
                        or is_low_value_external_url(source_url)
                    ):
                        continue
                elif not looks_like_external_search_result(result_url, title):
                    continue
                published = item.get("published")
                if isinstance(published, datetime) and published < cutoff:
                    continue
                results.setdefault(
                    result_url,
                    ExternalSearchResult(
                        url=result_url,
                        query=query,
                        provider=provider["name"],
                        title=title,
                        published=published if isinstance(published, datetime) else None,
                        source_url=source_url,
                    ),
                )
        except Exception as exc:
            logging.warning(
                "External search failed: provider=%s query=%s error=%s: %s",
                provider["name"],
                query,
                type(exc).__name__,
                exc,
            )
    return list(results.values())


def fetch_external_search_results() -> list[ExternalSearchResult]:
    merged: OrderedDict[str, ExternalSearchResult] = OrderedDict()
    provider_counts: dict[str, int] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as pool:
        future_map = {
            pool.submit(fetch_external_search_query, query, index): query
            for index, query in enumerate(EXTERNAL_SEARCH_QUERIES)
        }
        for future in concurrent.futures.as_completed(future_map):
            query = future_map[future]
            try:
                for result in future.result():
                    if result.url not in merged:
                        merged[result.url] = result
                        provider_counts[result.provider] = provider_counts.get(result.provider, 0) + 1
            except Exception as exc:
                logging.debug("External search query crashed: %s %s", query, exc)
    logging.info(
        "External search provider yield: %s",
        json.dumps(provider_counts, ensure_ascii=False, sort_keys=True),
    )
    return list(merged.values())


def is_initial_source_run(state: dict[str, Any], source: dict[str, str]) -> bool:
    source_state = source_state_for(state, source)
    return not source_state.get("last_success_at") and not source_state.get("last_links")


def should_recheck_priority_top(source_state: dict[str, Any], source: dict[str, str], run_at: datetime) -> bool:
    if PRIORITY_RECHECK_MAIN_TOP_N <= 0:
        return False
    if url_host(source.get("url", "")) not in TOP_RECHECK_SOURCE_HOSTS:
        return False
    last_recheck = parse_iso(source_state.get("last_top_recheck_at"))
    if not last_recheck:
        return True
    return last_recheck <= run_at - timedelta(minutes=PRIORITY_RECHECK_INTERVAL_MINUTES)


def should_recheck_recent_unsent_link(
    state: dict[str, Any],
    source: dict[str, str],
    link: str,
    index: int,
    run_at: datetime,
) -> bool:
    if RECENT_UNSENT_RECHECK_TOP_N <= 0 or index >= RECENT_UNSENT_RECHECK_TOP_N:
        return False
    if url_host(source.get("url", "")) not in TOP_RECHECK_SOURCE_HOSTS:
        return False

    seen_at = parse_iso(str(state.setdefault("seen_links", {}).get(link) or ""))
    if not seen_at:
        return False
    if seen_at <= run_at - timedelta(minutes=RECENT_UNSENT_RECHECK_MAX_AGE_MINUTES):
        return False

    rechecked = state.setdefault("recent_unsent_rechecks", {})
    last_recheck = parse_iso(str(rechecked.get(link) or ""))
    if last_recheck and last_recheck > run_at - timedelta(minutes=RECENT_UNSENT_RECHECK_INTERVAL_MINUTES):
        return False

    rechecked[link] = iso(run_at)
    return True


def supplementary_interval_minutes(source: dict[str, str]) -> int:
    if url_host(source.get("url", "")) in SUPPLEMENTARY_PRIORITY_HOSTS:
        return SUPPLEMENTARY_PRIORITY_INTERVAL_MINUTES
    return SUPPLEMENTARY_DISCOVERY_INTERVAL_MINUTES


def should_run_supplementary_discovery(
    state: dict[str, Any],
    source: dict[str, str],
    run_at: datetime,
) -> bool:
    source_state = source_state_for(state, source)
    last_run = parse_iso(source_state.get("last_supplementary_at"))
    interval = supplementary_interval_minutes(source)
    if last_run:
        return last_run <= run_at - timedelta(minutes=interval)

    local_minute = run_at.astimezone(ATYRAU_TZ).minute
    in_hourly_window = local_minute < SUPPLEMENTARY_BOOTSTRAP_WINDOW_MINUTES
    if url_host(source.get("url", "")) in SUPPLEMENTARY_PRIORITY_HOSTS:
        in_half_hour_window = 30 <= local_minute < 30 + SUPPLEMENTARY_BOOTSTRAP_WINDOW_MINUTES
        return in_hourly_window or in_half_hour_window
    return in_hourly_window


def mark_supplementary_discovery_attempt(
    state: dict[str, Any],
    source: dict[str, str],
    run_at: datetime,
) -> None:
    source_state_for(state, source)["last_supplementary_at"] = iso(run_at)


def candidate_links_for_source(
    state: dict[str, Any],
    fetched: SourceFetch,
    sent_canonicals: set[str],
    run_at: datetime,
) -> list[tuple[str, str]]:
    source_state = source_state_for(state, fetched.source)
    previous = set(source_state.get("last_links", []))
    seen = state.setdefault("seen_links", {})
    initial = is_initial_source_run(state, fetched.source)
    candidates: OrderedDict[str, str] = OrderedDict()
    recheck_priority_top = should_recheck_priority_top(source_state, fetched.source, run_at)
    top_recheck_added = False

    for index, link in enumerate(fetched.main_links[:MAIN_LINK_LIMIT]):
        if link in sent_canonicals or is_known_listing_url(link):
            continue
        should_recheck = (
            index < RECHECK_MAIN_TOP_N
            or (recheck_priority_top and index < PRIORITY_RECHECK_MAIN_TOP_N)
        )
        recent_unsent_recheck = (
            link in previous
            and should_recheck_recent_unsent_link(state, fetched.source, link, index, run_at)
        )
        if initial or link not in previous or should_recheck or recent_unsent_recheck:
            candidates[link] = "main"
            if should_recheck and link in previous:
                top_recheck_added = True

    if top_recheck_added:
        source_state["last_top_recheck_at"] = iso(run_at)

    supplementary_count = 0
    for link in fetched.supplementary_links:
        if link in sent_canonicals or link in seen or link in candidates or is_known_listing_url(link):
            continue
        if link not in seen:
            candidates[link] = "supplementary"
            supplementary_count += 1
        if supplementary_count >= MAX_SUPPLEMENTARY_CANDIDATES_PER_SOURCE:
            break

    return list(candidates.items())


def update_source_success(
    state: dict[str, Any],
    fetched: SourceFetch,
    run_at: datetime,
    verified_links: set[str] | None = None,
) -> None:
    source_state = source_state_for(state, fetched.source)
    previous = set(source_state.get("last_links", []))
    verified_links = verified_links or set()
    if not fetched.main_links:
        source_state["last_error"] = "no main links fetched"
        source_state["last_error_at"] = iso(run_at)
        return
    next_links = [
        link
        for link in fetched.main_links[:MAIN_LINK_LIMIT]
        if link in previous or link in verified_links
    ]
    if fetched.main_links and not next_links:
        source_state["last_error"] = "main links fetched but no article links verified"
        source_state["last_error_at"] = iso(run_at)
        return
    source_state["last_links"] = next_links
    source_state["last_success_at"] = iso(run_at)
    source_state.pop("last_error", None)


def update_source_error(state: dict[str, Any], fetched: SourceFetch, run_at: datetime) -> None:
    source_state = source_state_for(state, fetched.source)
    source_state["last_error"] = fetched.error
    source_state["last_error_at"] = iso(run_at)


def importance_level(score: int, very_strong_matches: list[str]) -> str:
    critical_terms = {
        "погиб",
        "погибли",
        "пострадали",
        "ДТП",
        "взрыв",
        "жарылыс",
        "ЧС",
        "разлив нефти",
        "коррупция",
        "задержание",
        "уголовное дело",
        "протест",
    }
    if not very_strong_matches and score >= 6:
        return "Средний"
    if score >= 16 or any(term in very_strong_matches for term in critical_terms):
        return "Критический" if score >= 12 else "Высокий"
    if score >= 10:
        return "Высокий"
    if score >= 6:
        return "Средний"
    return "Низкий"


def is_neutral_official_article(article: dict[str, Any]) -> bool:
    focused = "\n".join(
        normalize_space(str(part))
        for part in [
            article.get("headline", ""),
            article.get("description", ""),
            article.get("articleBody", "")[:700],
        ]
        if part
    )
    return bool(NEUTRAL_OFFICIAL_RE.search(focused))


def record_discovered_source(
    state: dict[str, Any],
    article: dict[str, Any],
    analysis: dict[str, Any],
    run_at: datetime,
    stats: dict[str, int] | None = None,
) -> None:
    canonical = canonicalize_url(article.get("canonical") or article.get("url") or "")
    host = url_host(canonical)
    if not host or host in SEARCH_AGGREGATOR_HOSTS or is_main_source_domain(canonical):
        return
    strong_region_matches = [
        term for term in (analysis.get("region_matches") or []) if term not in WEAK_REGION_TERMS
    ]
    if not strong_region_matches and not analysis.get("org_matches"):
        return

    discovered = state.setdefault("discovered_sources", {})
    is_new = host not in discovered
    info = discovered.setdefault(
        host,
        {
            "domain": host,
            "source_url": source_root_url_from_host(host),
            "discovered_at": iso(run_at),
            "found_count": 0,
            "relevant_count": 0,
            "relevance_score": 0,
            "promoted": False,
            "sample_urls": [],
        },
    )
    info["domain"] = host
    info.setdefault("source_url", source_root_url_from_host(host))
    info["last_seen_at"] = iso(run_at)
    info["found_count"] = int(info.get("found_count") or 0) + 1
    score = int(analysis.get("importance_score") or 0)
    if analysis.get("significant"):
        info["relevant_count"] = int(info.get("relevant_count") or 0) + 1
    info["relevance_score"] = int(info.get("relevance_score") or 0) + score
    samples = [str(url) for url in info.get("sample_urls", []) if isinstance(url, str)]
    if canonical and canonical not in samples:
        samples.append(canonical)
    info["sample_urls"] = samples[-DISCOVERED_SOURCE_SAMPLE_LIMIT:]
    if is_new:
        bump_counter(stats, "new_sources_found")
        logging.info("New external source discovered: host=%s score=%s url=%s", host, score, canonical)

    should_promote = (
        int(info.get("relevant_count") or 0) >= DISCOVERED_SOURCE_PROMOTE_RELEVANT_COUNT
        or (
            int(info.get("found_count") or 0) >= DISCOVERED_SOURCE_PROMOTE_FOUND_COUNT
            and int(info.get("relevance_score") or 0) >= DISCOVERED_SOURCE_PROMOTE_SCORE
        )
    )
    if should_promote and not info.get("promoted"):
        info["promoted"] = True
        info["promoted_at"] = iso(run_at)
        bump_counter(stats, "new_sources_promoted")
        logging.info(
            "External source promoted to permanent monitoring: host=%s found_count=%s relevant_count=%s relevance_score=%s",
            host,
            info.get("found_count"),
            info.get("relevant_count"),
            info.get("relevance_score"),
        )


def analyze_article(article: dict[str, Any]) -> dict[str, Any]:
    text = article.get("combinedText", "")
    headline = article.get("headline", "")
    focused_text = "\n".join(
        normalize_space(str(part))
        for part in [
            article.get("headline", ""),
            article.get("description", ""),
            str(article.get("articleBody") or "")[:1800],
        ]
        if part
    )
    raw_region_matches = text_contains_any(text, REGIONAL_TERMS, REGIONAL_STEM_TERMS)
    region_matches = refine_region_matches(article, raw_region_matches)
    focused_region_matches = refine_region_matches(
        article,
        text_contains_any(focused_text, REGIONAL_TERMS, REGIONAL_STEM_TERMS),
    )
    org_matches = text_contains_any(text, ORGANIZATION_TERMS)
    risk_matches = text_contains_any(text, RISK_TERMS, STEM_TERMS)
    very_strong_matches = text_contains_any(text, VERY_STRONG_SIGNAL_TERMS, SIGNAL_STEM_TERMS)
    medium_signal_matches = text_contains_any(text, MEDIUM_SIGNAL_TERMS, SIGNAL_STEM_TERMS)
    weak_signal_matches = text_contains_any(text, WEAK_SIGNAL_TERMS, SIGNAL_STEM_TERMS)
    resonant_financial_person_matches = text_contains_any(
        focused_text,
        RESONANT_FINANCIAL_PERSON_TERMS,
        RESONANT_FINANCIAL_STEM_TERMS,
    )
    resonant_financial_money_matches = text_contains_any(
        focused_text,
        RESONANT_FINANCIAL_MONEY_TERMS,
        RESONANT_FINANCIAL_STEM_TERMS,
    )
    unique_org_matches = [term for term in org_matches if term not in GENERIC_ORGANIZATION_TERMS]
    strong_region_matches = [term for term in region_matches if term not in WEAK_REGION_TERMS]
    focused_strong_region_matches = [
        term for term in focused_region_matches if term not in WEAK_REGION_TERMS
    ]
    weak_region_matches = [term for term in region_matches if term in WEAK_REGION_TERMS]
    hard_risk_matches = [term for term in risk_matches if term in HARD_RISK_TERMS]
    weak_region_environment_risk_matches = [
        term for term in risk_matches if term in WEAK_REGION_ENVIRONMENT_RISK_TERMS
    ]
    resonant_financial_matches = []
    if strong_region_matches and resonant_financial_person_matches and resonant_financial_money_matches:
        resonant_financial_matches = list(
            OrderedDict.fromkeys(
                resonant_financial_person_matches[:3] + resonant_financial_money_matches[:3]
            )
        )
    signal_score = (
        len(very_strong_matches) * 5
        + len(medium_signal_matches) * 3
        + len(weak_signal_matches)
        + (4 if resonant_financial_matches else 0)
    )
    region_score = 3 if strong_region_matches else 0
    if weak_region_matches and weak_region_environment_risk_matches:
        region_score += 1
    org_score = min(4, len(unique_org_matches) * 2)
    score = signal_score + region_score + org_score
    has_region_context = bool(
        strong_region_matches
        or unique_org_matches
        or (weak_region_matches and weak_region_environment_risk_matches)
    )
    has_actionable_signal = bool(
        very_strong_matches or medium_signal_matches or resonant_financial_matches
    )
    significant = bool(has_region_context and has_actionable_signal and score >= 5)

    if headline and SKIP_HEADLINE_RE.search(headline):
        significant = False
    if (
        significant
        and not very_strong_matches
        and not medium_signal_matches
        and not resonant_financial_matches
    ):
        significant = False
    if (
        significant
        and not very_strong_matches
        and not resonant_financial_matches
        and not hard_risk_matches
        and set(risk_matches).issubset(COURT_ONLY_TERMS | WEAK_RISK_TERMS)
    ):
        significant = False
    if significant and headline and ADMIN_NEUTRAL_HEADLINE_RE.search(headline) and not very_strong_matches:
        significant = False
    if significant and is_neutral_official_article(article):
        headline_strong_matches = text_contains_any(
            headline,
            VERY_STRONG_SIGNAL_TERMS,
            SIGNAL_STEM_TERMS,
        )
        if (
            not resonant_financial_matches
            and not headline_strong_matches
            and not any(term in NEUTRAL_OFFICIAL_ESCALATION_TERMS for term in very_strong_matches)
        ):
            significant = False
    if (
        significant
        and not is_local_region_source(article)
        and strong_region_matches
        and not focused_strong_region_matches
        and not unique_org_matches
    ):
        significant = False
    level = importance_level(score, very_strong_matches)
    reasons = []
    if region_matches:
        reasons.append("региональная связь: " + ", ".join(region_matches[:5]))
    if org_matches:
        reasons.append("упомянуты организации: " + ", ".join(org_matches[:5]))
    if very_strong_matches:
        reasons.append("сильные сигналы: " + ", ".join(very_strong_matches[:6]))
    if medium_signal_matches:
        reasons.append("средние сигналы: " + ", ".join(medium_signal_matches[:6]))
    if resonant_financial_matches:
        reasons.append("финансовый резонанс: " + ", ".join(resonant_financial_matches[:6]))
    if weak_signal_matches:
        reasons.append("слабые сигналы: " + ", ".join(weak_signal_matches[:4]))
    if risk_matches and not (very_strong_matches or medium_signal_matches):
        reasons.append("тема повышенного внимания: " + ", ".join(risk_matches[:6]))

    return {
        "significant": significant,
        "region_matches": region_matches,
        "org_matches": org_matches,
        "risk_matches": risk_matches,
        "very_strong_matches": very_strong_matches,
        "medium_signal_matches": medium_signal_matches,
        "weak_signal_matches": weak_signal_matches,
        "resonant_financial_matches": resonant_financial_matches,
        "importance_score": score,
        "importance_level": level,
        "reason": "; ".join(reasons) if reasons else "",
    }


def is_advertorial(article: dict[str, Any]) -> bool:
    canonical = article.get("canonical") or article.get("url") or ""
    if AD_URL_RE.search(urllib.parse.urlsplit(canonical).path):
        return True
    focused_text = "\n".join(
        normalize_space(str(part))
        for part in [
            article.get("headline", ""),
            article.get("description", ""),
            article.get("keywords", ""),
            " ".join(article.get("tags", [])),
            article.get("articleBody", "")[:900],
        ]
        if part
    )
    return bool(AD_LABEL_RE.search(focused_text))


def normalize_published_for_run(published: datetime | None, run_at: datetime) -> datetime | None:
    if not published:
        return None
    future_tolerance = timedelta(minutes=FUTURE_DATE_TOLERANCE_MINUTES)
    if published <= run_at + future_tolerance:
        return published

    offset = ATYRAU_TZ.utcoffset(run_at.astimezone(ATYRAU_TZ)) or timedelta(hours=5)
    adjusted = published - offset
    if adjusted <= run_at + future_tolerance and adjusted >= run_at - timedelta(days=7):
        return adjusted
    return published


def article_published_at(article: dict[str, Any], run_at: datetime) -> datetime | None:
    return normalize_published_for_run(parse_iso(article.get("datePublished")), run_at)


def format_article_published_for_message(article: dict[str, Any], run_at: datetime) -> str:
    published = article_published_at(article, run_at)
    if not published:
        return "не указана"
    return published.astimezone(ATYRAU_TZ).strftime("%d.%m.%Y %H:%M")


def article_delay_minutes(article: dict[str, Any], detected_at: datetime) -> int | None:
    published = article_published_at(article, detected_at)
    if not published:
        return None
    return max(0, int((detected_at - published).total_seconds() // 60))


def origin_label(origin: str) -> str:
    return "внешний поиск" if origin == EXTERNAL_SEARCH_ORIGIN else "основной источник"


def external_no_date_is_strong(article: dict[str, Any], analysis: dict[str, Any], detected_at: datetime) -> bool:
    if article_published_at(article, detected_at):
        return True
    url = str(article.get("canonical") or article.get("url") or "")
    local = detected_at.astimezone(ATYRAU_TZ)
    recent_years = {str(local.year), str((local - timedelta(days=2)).year)}
    if not any(year in urllib.parse.urlsplit(url).path for year in recent_years):
        return False
    region_matches = analysis.get("region_matches") or []
    risk_matches = analysis.get("risk_matches") or []
    strong_region_matches = [term for term in region_matches if term not in WEAK_REGION_TERMS]
    strong_negative_matches = [
        term
        for term in risk_matches
        if term in EXTERNAL_NO_DATE_STRONG_RISK_TERMS
    ]
    return bool(strong_region_matches and strong_negative_matches)


def summarize_article(article: dict[str, Any]) -> str:
    text = normalize_space(article.get("description") or article.get("articleBody") or "")
    if not text:
        return "В материале обнаружена связь с Атырауской областью и темой повышенного внимания."
    headline = normalize_space(article.get("headline") or "")
    if headline and text.casefold().startswith(headline.casefold()):
        text = normalize_space(text[len(headline) :])
    max_len = 420
    if len(text) > max_len:
        text = text[: max_len - 1].rsplit(" ", 1)[0] + "…"
    return text


def build_alert_message(article: dict[str, Any], analysis: dict[str, Any], detected_at: datetime) -> str:
    delay_minutes = article_delay_minutes(article, detected_at)
    importance = analysis.get("reason") or "общественно значимая публикация по региону"
    if delay_minutes is not None and delay_minutes > DELAY_FAILURE_MINUTES:
        importance = f"{importance}; обнаружено с задержкой: {delay_minutes} минут"
    found_via = article.get("foundVia") or "основной источник"
    level = analysis.get("importance_level") or "Средний"

    return (
        f"Заголовок: {normalize_space(article.get('headline'))}\n"
        "\n"
        f"Источник: {article.get('source')}\n"
        f"Дата публикации: {format_article_published_for_message(article, detected_at)}\n"
        f"Краткое содержание: {summarize_article(article)}\n"
        f"Причина важности: {importance}\n"
        f"Уровень важности: {level}\n"
        f"Ссылка: {article.get('canonical') or article.get('url')}\n"
        f"Откуда найдено: {found_via}\n\n"
        "Айбар красавчик."
    )


def is_too_old_to_alert(article: dict[str, Any], detected_at: datetime) -> bool:
    published = article_published_at(article, detected_at)
    if not published:
        return False
    delay_minutes = int((detected_at - published).total_seconds() // 60)
    return delay_minutes > MAX_ALERT_DELAY_MINUTES


def sanitize_telegram_text(text: str, limit: int = 3900) -> str:
    safe = "".join(
        ch
        for ch in text
        if ch in "\n\t" or (ord(ch) >= 32 and not 0xD800 <= ord(ch) <= 0xDFFF)
    )
    if len(safe) <= limit:
        return safe
    suffix = "\n\n[сообщение сокращено]"
    return safe[: limit - len(suffix)].rsplit("\n", 1)[0] + suffix


def telegram_send(token: str, chat_id: str, text: str) -> None:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = sanitize_telegram_text(text)
    for attempt in range(2):
        data = urllib.parse.urlencode(
            {
                "chat_id": chat_id,
                "text": text,
                "disable_web_page_preview": "false",
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            url,
            data=data,
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=FETCH_TIMEOUT) as response:
                body = response.read(MAX_BYTES)
                if response.status >= 300:
                    raise RuntimeError(f"Telegram HTTP {response.status}: {body[:300]!r}")
                parsed = json.loads(body.decode("utf-8", errors="replace"))
                if not parsed.get("ok"):
                    raise RuntimeError(f"Telegram API error: {parsed}")
                return
        except urllib.error.HTTPError as exc:
            body = exc.read(1200).decode("utf-8", errors="replace")
            migrate_to = None
            try:
                payload = json.loads(body)
                migrate_to = payload.get("parameters", {}).get("migrate_to_chat_id")
            except json.JSONDecodeError:
                pass
            if migrate_to and attempt == 0:
                chat_id = str(migrate_to)
                update_env_value(ENV_PATH, "TELEGRAM_CHAT_ID", chat_id)
                logging.info("Telegram chat migrated; TELEGRAM_CHAT_ID updated")
                continue
            logging.error("Telegram send failed: HTTP %s %s", exc.code, body)
            raise RuntimeError(f"Telegram HTTP {exc.code}: {body}") from exc


def should_alert_on_initial_run(article: dict[str, Any], run_at: datetime) -> bool:
    published = article_published_at(article, run_at)
    if not published:
        return False
    return published >= run_at - timedelta(minutes=INITIAL_LOOKBACK_MINUTES)


def fetch_article(url: str, source_name: str) -> tuple[str, dict[str, Any] | None, str | None]:
    try:
        if is_known_non_article_url(url):
            return url, None, "known non-article page"
        final_url, content_type, html_text = fetch_url(url)
        if "html" not in content_type.casefold() and "<html" not in html_text[:1000].casefold():
            return url, None, f"not html: {content_type}"
        article = extract_article(final_url, html_text, source_name)
        if is_known_non_article_url(article.get("canonical") or final_url):
            return url, None, "known non-article page"
        return url, article, None
    except Exception as exc:
        return url, None, f"{type(exc).__name__}: {exc}"


def send_daily_heartbeat_if_needed(state: dict[str, Any], token: str, chat_id: str, run_at: datetime, dry_run: bool) -> None:
    local = run_at.astimezone(ATYRAU_TZ)
    today = local.date().isoformat()
    if local.hour != 9 or local.minute >= 15:
        return
    if state.get("last_heartbeat_date") == today:
        return
    last_sent = parse_iso(state.get("last_significant_sent_at"))
    if last_sent and last_sent >= run_at - timedelta(hours=24):
        return
    text = (
        "✅ Мониторинг работает.\n"
        "За последние 24 часа новых значимых публикаций не обнаружено.\n\n"
        "Айбар красавчик."
    )
    if dry_run:
        logging.info("DRY RUN daily heartbeat: %s", text)
    else:
        telegram_send(token, chat_id, text)
    state["last_heartbeat_date"] = today


def bump_counter(stats: dict[str, int] | None, key: str, amount: int = 1) -> None:
    if stats is not None:
        stats[key] = stats.get(key, 0) + amount


DISCARDED_STAT_KEYS = (
    "fetch_error",
    "fetch_crashed",
    "empty_article",
    "duplicate_canonical",
    "duplicate_signature",
    "external_main_source_canonical",
    "advertorial",
    "not_significant",
    "external_no_date_weak",
    "too_old",
    "initial_old",
    "external_duplicate_prefetch",
    "external_non_article_prefetch",
    "external_low_value_prefetch",
    "external_main_source_prefetch",
    "external_filtered_aggregator",
    "external_cap_dropped",
)


def discarded_count(stats: dict[str, int]) -> int:
    return sum(int(stats.get(key, 0)) for key in DISCARDED_STAT_KEYS)


def process_article_jobs(
    jobs: OrderedDict[str, tuple[str, bool, str]],
    run_at: datetime,
    token: str,
    chat_id: str,
    state: dict[str, Any],
    sent_canonicals: set[str],
    external_result_meta: dict[str, ExternalSearchResult],
    dry_run: bool,
    dry_run_alerts: list[dict[str, str]] | None = None,
    stats: dict[str, int] | None = None,
    monitor_sources: list[dict[str, str]] | None = None,
) -> tuple[int, int, dict[str, set[str]]]:
    seen = state.setdefault("seen_links", {})
    alerts_sent = 0
    articles_checked = 0
    verified_links_by_source: dict[str, set[str]] = {}

    if not jobs:
        return alerts_sent, articles_checked, verified_links_by_source

    with concurrent.futures.ThreadPoolExecutor(max_workers=ARTICLE_WORKERS) as pool:
        future_map = {
            pool.submit(fetch_article, link, source_name): (link, source_name, initial, origin)
            for link, (source_name, initial, origin) in jobs.items()
        }
        bump_counter(stats, "article_jobs", len(future_map))
        for future in concurrent.futures.as_completed(future_map):
            link, source_name, initial, origin = future_map[future]
            try:
                _url, article, error = future.result()
            except Exception as exc:
                logging.warning("Article fetch crashed: %s %s", link, exc)
                bump_counter(stats, "fetch_crashed")
                continue
            if error:
                logging.debug("Article skipped: %s %s", link, error)
                bump_counter(stats, "fetch_error")
                continue
            if not article:
                bump_counter(stats, "empty_article")
                continue
            if origin != EXTERNAL_SEARCH_ORIGIN:
                verified_links_by_source.setdefault(source_name, set()).add(link)
            seen.setdefault(link, iso(run_at))
            articles_checked += 1
            if origin == EXTERNAL_SEARCH_ORIGIN:
                meta = external_result_meta.get(link)
                if meta and not article.get("datePublished") and meta.published:
                    article["datePublished"] = iso(meta.published)
            canonical = canonicalize_url(article.get("canonical") or article.get("url") or link)
            article["canonical"] = canonical
            if origin == EXTERNAL_SEARCH_ORIGIN:
                article["source"] = source_name_from_url(canonical)
            seen.setdefault(canonical, iso(run_at))
            duplicate_reason = duplicate_article_reason(state, article, canonical, run_at, sent_canonicals)
            if duplicate_reason:
                bump_counter(stats, "duplicate_canonical" if duplicate_reason == "canonical" else "duplicate_signature")
                continue
            if origin == EXTERNAL_SEARCH_ORIGIN and is_monitored_source_domain(canonical, monitor_sources or SOURCES):
                bump_counter(stats, "external_main_source_canonical")
                continue
            if is_advertorial(article):
                logging.info("Advertorial skipped: %s", canonical)
                bump_counter(stats, "advertorial")
                continue
            analysis = analyze_article(article)
            if origin == EXTERNAL_SEARCH_ORIGIN:
                record_discovered_source(state, article, analysis, run_at, stats)
            if not analysis["significant"]:
                bump_counter(stats, "not_significant")
                continue
            if origin == EXTERNAL_SEARCH_ORIGIN and not external_no_date_is_strong(article, analysis, run_at):
                logging.info("External no-date weak candidate skipped: %s", canonical)
                bump_counter(stats, "external_no_date_weak")
                continue
            if is_too_old_to_alert(article, run_at):
                logging.info("Too-old alert skipped: %s", canonical)
                bump_counter(stats, "too_old")
                continue
            if initial and not should_alert_on_initial_run(article, run_at):
                logging.info("Initial baseline significant-but-old skipped: %s", canonical)
                bump_counter(stats, "initial_old")
                continue
            delay_minutes = article_delay_minutes(article, run_at)
            article["foundVia"] = origin_label(origin)
            logging.info(
                "Alert candidate: origin=%s source=%s canonical=%s delay_minutes=%s reason=%s",
                origin,
                source_name,
                canonical,
                delay_minutes,
                analysis.get("reason") or "",
            )
            message = build_alert_message(article, analysis, run_at)
            if dry_run:
                if dry_run_alerts is not None:
                    dry_run_alerts.append(
                        {
                            "source": normalize_space(str(article.get("source") or source_name)),
                            "headline": normalize_space(str(article.get("headline") or "")),
                            "url": canonical,
                            "origin": origin_label(origin),
                            "reason": normalize_space(str(analysis.get("reason") or "")),
                            "importance_level": str(analysis.get("importance_level") or ""),
                        }
                    )
                logging.info("DRY RUN alert candidate suppressed from log")
            else:
                telegram_send(token, chat_id, message)
                append_sent_canonical(canonical)
                sent_canonicals.add(canonical)
                remember_sent_article(state, article, analysis, canonical, run_at)
                state["last_significant_sent_at"] = iso(run_at)
            alerts_sent += 1
            bump_counter(stats, "relevant")

    return alerts_sent, articles_checked, verified_links_by_source


def run_once(send_test: bool = False, dry_run: bool = False) -> int:
    setup_logging()
    if not acquire_run_lock():
        logging.info("Another monitor cycle is already running; skipping this run")
        return 0
    try:
        return _run_once_locked(send_test=send_test, dry_run=dry_run)
    finally:
        release_run_lock()


def _run_once_locked(send_test: bool = False, dry_run: bool = False) -> int:
    run_at = now_utc()
    env = load_env(ENV_PATH)
    token = env.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = env.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logging.error("Telegram credentials are missing in .env or environment")
        return 2

    state = load_state()
    sent_canonicals = load_sent_canonicals()
    monitor_sources = monitor_sources_for_state(state)

    if send_test:
        published = run_at.astimezone(ATYRAU_TZ).strftime("%d.%m.%Y %H:%M")
        text = (
            "Заголовок: Тестовое сообщение мониторинга\n"
            "\n"
            "Источник: AtyrauMonitor\n"
            f"Дата публикации: {published}\n"
            "Краткое содержание: Проверка нового формата Telegram-сообщения.\n"
            "Причина важности: тест отправки после изменения формата\n"
            "Уровень важности: Низкий\n"
            "Ссылка: https://example.com/atyrau-monitor-test\n"
            "Откуда найдено: основной источник\n"
            "\n"
            "Айбар красавчик."
        )
        if dry_run:
            logging.info("DRY RUN test message: %s", text)
        else:
            telegram_send(token, chat_id, text)

    logging.info("Starting monitor cycle")
    cycle_started = time.monotonic()
    seen = state.setdefault("seen_links", {})
    external_result_meta: dict[str, ExternalSearchResult] = {}
    dry_run_alerts: list[dict[str, str]] = []
    article_stats: dict[str, int] = {}
    external_raw_results = 0
    external_duplicates = 0
    external_filtered_non_article = 0
    external_filtered_aggregator = 0
    external_filtered_main_source = 0
    external_dropped = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as pool:
        fetched_sources = list(pool.map(fetch_source_main, monitor_sources))
    main_feeds_seconds = time.monotonic() - cycle_started
    ok_sources = sum(1 for fetched in fetched_sources if fetched.ok)
    logging.info(
        "Main feeds fetched: sources=%s ok=%s elapsed_seconds=%.1f",
        len(fetched_sources),
        ok_sources,
        main_feeds_seconds,
    )

    main_jobs: OrderedDict[str, tuple[str, bool, str]] = OrderedDict()
    for fetched in fetched_sources:
        if not fetched.ok:
            logging.warning("Source failed: %s %s", fetched.source["name"], fetched.error)
            continue
        source_initial = is_initial_source_run(state, fetched.source)
        for link, origin in candidate_links_for_source(state, fetched, sent_canonicals, run_at):
            if origin == "main":
                main_jobs.setdefault(link, (fetched.source["name"], source_initial, origin))

    alerts_sent, articles_checked, verified_links_by_source = process_article_jobs(
        main_jobs,
        run_at,
        token,
        chat_id,
        state,
        sent_canonicals,
        external_result_meta,
        dry_run,
        dry_run_alerts,
        article_stats,
        monitor_sources,
    )
    main_latency_seconds = time.monotonic() - cycle_started
    logging.info(
        "Main layer finished: candidate_links=%s articles_checked=%s alerts_sent=%s main_latency_seconds=%.1f",
        len(main_jobs),
        articles_checked,
        alerts_sent,
        main_latency_seconds,
    )

    supplementary_due_sources: list[SourceFetch] = []
    for fetched in fetched_sources:
        if fetched.ok and should_run_supplementary_discovery(state, fetched.source, run_at):
            mark_supplementary_discovery_attempt(state, fetched.source, run_at)
            supplementary_due_sources.append(fetched)
    logging.info(
        "Supplementary discovery scheduled: due_sources=%s skipped_sources=%s priority_interval_minutes=%s regular_interval_minutes=%s",
        len(supplementary_due_sources),
        len(fetched_sources) - len(supplementary_due_sources),
        SUPPLEMENTARY_PRIORITY_INTERVAL_MINUTES,
        SUPPLEMENTARY_DISCOVERY_INTERVAL_MINUTES,
    )

    def fetch_all_supplementary_sources() -> list[SourceFetch]:
        if not supplementary_due_sources:
            return fetched_sources
        with concurrent.futures.ThreadPoolExecutor(max_workers=SOURCE_WORKERS) as pool:
            supplemented_due = list(pool.map(fetch_source_supplementary, supplementary_due_sources))
        supplemented_by_key = {
            source_state_key(fetched.source): fetched
            for fetched in supplemented_due
        }
        return [
            supplemented_by_key.get(source_state_key(fetched.source), fetched)
            for fetched in fetched_sources
        ]

    supplementary_started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as layer_pool:
        supplementary_future = layer_pool.submit(fetch_all_supplementary_sources)
        external_future = layer_pool.submit(fetch_external_search_results)

        try:
            external_results = external_future.result()
        except Exception as exc:
            logging.warning("External search crashed: %s", exc)
            external_results = []

        external_jobs: OrderedDict[str, tuple[str, bool, str]] = OrderedDict()
        external_raw_results = len(external_results)
        external_aggregator_resolutions = 0
        external_aggregator_resolutions_by_host: dict[str, int] = {}
        for index, result in enumerate(external_results):
            if len(external_jobs) >= MAX_EXTERNAL_SEARCH_CANDIDATES_PER_CYCLE:
                dropped_now = len(external_results) - index
                external_dropped += dropped_now
                bump_counter(article_stats, "external_cap_dropped", dropped_now)
                break
            result_is_resolvable_aggregator = is_resolvable_search_aggregator_url(result.url)
            if result_is_resolvable_aggregator:
                source_host = url_host(result.source_url)
                if not promising_aggregator_result(result):
                    external_filtered_aggregator += 1
                    bump_counter(article_stats, "external_filtered_aggregator")
                    continue
                if external_aggregator_resolutions >= MAX_EXTERNAL_AGGREGATOR_RESOLUTION_PER_CYCLE:
                    external_filtered_aggregator += 1
                    bump_counter(article_stats, "external_filtered_aggregator")
                    continue
                if (
                    source_host
                    and external_aggregator_resolutions_by_host.get(source_host, 0)
                    >= MAX_EXTERNAL_AGGREGATOR_RESOLUTION_PER_HOST
                ):
                    external_filtered_aggregator += 1
                    bump_counter(article_stats, "external_filtered_aggregator")
                    continue
                external_aggregator_resolutions += 1
                if source_host:
                    external_aggregator_resolutions_by_host[source_host] = (
                        external_aggregator_resolutions_by_host.get(source_host, 0) + 1
                    )
            normalized_result = normalize_external_result(result)
            if not normalized_result:
                external_filtered_aggregator += 1
                bump_counter(article_stats, "external_filtered_aggregator")
                continue
            result = normalized_result
            if (
                result.url in seen
                or result.url in sent_canonicals
                or result.url in main_jobs
            ):
                external_duplicates += 1
                bump_counter(article_stats, "external_duplicate_prefetch")
                continue
            if result.url in external_jobs:
                external_duplicates += 1
                bump_counter(article_stats, "external_duplicate_prefetch")
                continue
            if is_known_non_article_url(result.url):
                external_filtered_non_article += 1
                bump_counter(article_stats, "external_non_article_prefetch")
                continue
            if is_low_value_external_url(result.url):
                external_filtered_non_article += 1
                bump_counter(article_stats, "external_low_value_prefetch")
                continue
            if is_monitored_source_domain(result.url, monitor_sources):
                external_filtered_main_source += 1
                bump_counter(article_stats, "external_main_source_prefetch")
                continue
            external_result_meta[result.url] = result
            external_jobs[result.url] = (source_name_from_url(result.url), False, EXTERNAL_SEARCH_ORIGIN)
        logging.info(
            "External search finished: queries=%s results=%s candidates=%s duplicates=%s aggregator_skipped=%s main_source_skipped=%s non_article_skipped=%s aggregator_resolutions=%s",
            len(EXTERNAL_SEARCH_QUERIES),
            external_raw_results,
            len(external_jobs),
            external_duplicates,
            external_filtered_aggregator,
            external_filtered_main_source,
            external_filtered_non_article,
            external_aggregator_resolutions,
        )
        if external_dropped:
            logging.info(
                "External search candidates capped: kept=%s dropped=%s",
                MAX_EXTERNAL_SEARCH_CANDIDATES_PER_CYCLE,
                external_dropped,
            )

        external_alerts, external_checked, external_verified = process_article_jobs(
            external_jobs,
            run_at,
            token,
            chat_id,
            state,
            sent_canonicals,
            external_result_meta,
            dry_run,
            dry_run_alerts,
            article_stats,
            monitor_sources,
        )
        alerts_sent += external_alerts
        articles_checked += external_checked
        for source_name, links in external_verified.items():
            verified_links_by_source.setdefault(source_name, set()).update(links)

        try:
            supplemented_sources = supplementary_future.result()
        except Exception as exc:
            logging.warning("Supplementary discovery crashed: %s", exc)
            supplemented_sources = fetched_sources

    supplementary_discovery_seconds = time.monotonic() - supplementary_started
    supplementary_jobs: OrderedDict[str, tuple[str, bool, str]] = OrderedDict()
    supplementary_dropped = 0
    for fetched in supplemented_sources:
        if not fetched.ok:
            continue
        source_initial = is_initial_source_run(state, fetched.source)
        per_source_kept = 0
        for link in fetched.supplementary_links:
            if (
                link in seen
                or link in sent_canonicals
                or link in main_jobs
                or is_known_non_article_url(link)
            ):
                continue
            if per_source_kept >= MAX_SUPPLEMENTARY_CANDIDATES_PER_SOURCE:
                supplementary_dropped += 1
                continue
            if len(supplementary_jobs) >= MAX_SUPPLEMENTARY_CANDIDATES_PER_CYCLE:
                supplementary_dropped += 1
                continue
            if link in supplementary_jobs:
                continue
            supplementary_jobs[link] = (fetched.source["name"], source_initial, "supplementary")
            per_source_kept += 1
    logging.info(
        "Supplementary discovery finished: candidate_links=%s dropped=%s elapsed_seconds=%.1f",
        len(supplementary_jobs),
        supplementary_dropped,
        supplementary_discovery_seconds,
    )

    supplementary_alerts, supplementary_checked, supplementary_verified = process_article_jobs(
        supplementary_jobs,
        run_at,
        token,
        chat_id,
        state,
        sent_canonicals,
        external_result_meta,
        dry_run,
        dry_run_alerts,
        article_stats,
        monitor_sources,
    )
    alerts_sent += supplementary_alerts
    articles_checked += supplementary_checked
    for source_name, links in supplementary_verified.items():
        verified_links_by_source.setdefault(source_name, set()).update(links)

    for fetched in supplemented_sources:
        if fetched.ok:
            update_source_success(
                state,
                fetched,
                run_at,
                verified_links_by_source.get(fetched.source["name"], set()),
            )
        else:
            update_source_error(state, fetched, run_at)
    state["last_successful_run_at"] = iso(run_at)

    send_daily_heartbeat_if_needed(state, token, chat_id, run_at, dry_run)
    discarded = discarded_count(article_stats)
    logging.info(
        "Cycle stats: sources_checked=%s materials_found=%s external_found=%s duplicates=%s relevant=%s sent=%s would_send=%s new_sources=%s promoted_sources=%s discarded=%s stats=%s",
        len(fetched_sources),
        len(main_jobs) + len(external_jobs) + len(supplementary_jobs),
        external_raw_results,
        article_stats.get("duplicate_canonical", 0)
        + article_stats.get("duplicate_signature", 0)
        + article_stats.get("external_duplicate_prefetch", 0),
        article_stats.get("relevant", 0),
        0 if dry_run else alerts_sent,
        alerts_sent if dry_run else 0,
        article_stats.get("new_sources_found", 0),
        article_stats.get("new_sources_promoted", 0),
        discarded,
        json.dumps(article_stats, ensure_ascii=False, sort_keys=True),
    )
    if not dry_run:
        save_state(state)
    if dry_run:
        logging.info(
            "DRY RUN summary: sources_checked=%s primary_candidates=%s main_candidates=%s supplementary_candidates=%s external_results=%s external_candidates=%s discarded=%s external_duplicates=%s relevant=%s new_sources=%s promoted_sources=%s stats=%s",
            len(fetched_sources),
            len(main_jobs) + len(supplementary_jobs),
            len(main_jobs),
            len(supplementary_jobs),
            external_raw_results,
            len(external_jobs),
            discarded,
            external_duplicates,
            len(dry_run_alerts),
            article_stats.get("new_sources_found", 0),
            article_stats.get("new_sources_promoted", 0),
            json.dumps(article_stats, ensure_ascii=False, sort_keys=True),
        )
        for record in dry_run_alerts:
            logging.info(
                "DRY RUN would send: source=%s title=%s url=%s origin=%s level=%s reason=%s",
                record.get("source", ""),
                record.get("headline", ""),
                record.get("url", ""),
                record.get("origin", ""),
                record.get("importance_level", ""),
                record.get("reason", ""),
            )
    logging.info(
        "Cycle finished: sources=%s candidate_links=%s main_candidate_links=%s articles_checked=%s alerts_sent=%s would_send=%s main_latency_seconds=%.1f total_seconds=%.1f",
        len(supplemented_sources),
        len(main_jobs) + len(external_jobs) + len(supplementary_jobs),
        len(main_jobs),
        articles_checked,
        0 if dry_run else alerts_sent,
        alerts_sent if dry_run else 0,
        main_latency_seconds,
        time.monotonic() - cycle_started,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Atyrau media monitor")
    parser.add_argument("--once", action="store_true", help="run one monitoring cycle")
    parser.add_argument("--send-test", action="store_true", help="send Telegram connection test")
    parser.add_argument("--dry-run", action="store_true", help="do not send Telegram messages")
    args = parser.parse_args()
    if not args.once and not args.send_test:
        parser.error("use --once and/or --send-test")
    try:
        return run_once(send_test=args.send_test, dry_run=args.dry_run)
    except KeyboardInterrupt:
        return 130
    except Exception:
        setup_logging()
        logging.error("Fatal monitor error:\n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
