"""Route weights and per-request reservations.

Billing only trusts the template the framework reports: at the end of a request the weight is looked up by exact "method + template", and anything the seller didn't configure counts as 1.
Frameworks that know the template when the request starts (Flask, Django) reserve by it too; where it isn't known (FastAPI middleware runs before routing),
the path is matched against the seller's templates at the start, and that guess only sets the reservation. Same rules as routes.ts in the Node SDK,
plus two more parameter styles: Starlette's {id} and {path:path}, and Flask's and Django's <id>, <int:id> and <path:rest>.
"""

from __future__ import annotations

import re
from typing import NamedTuple

from .protocol import RouteSetting


class _Pattern(NamedTuple):
    setting: RouteSetting
    method: str
    regex: re.Pattern[str]


# {name} / {name:converter}（Starlette）；<name> / <converter:name> / <converter(args):name>（Flask、Django）
_PARAMETER = re.compile(r"\{(?P<brace>[^{}:]+)(?::(?P<brace_converter>[^{}]+))?\}|<(?:(?P<angle_converter>[^<>:]+):)?(?P<angle>[^<>]+)>")


class RouteTable:
    def __init__(self, settings: list[RouteSetting]) -> None:
        self._exact: dict[str, RouteSetting] = {}
        # Templates without parameters, stored again without a trailing slash, for matching paths at the start. Not everything becomes a regex: a seller can configure up to 500 routes, and every request has to be matched against them
        self._literal: dict[str, list[RouteSetting]] = {}
        self._patterns: list[_Pattern] = []
        for setting in settings:
            method = _normalize_method(setting["method"])
            self._exact[f"{method} {setting['route']}"] = setting
            if _has_parameters(setting["route"]):
                self._patterns.append(_Pattern(setting, method, _template_to_regex(setting["route"])))
            else:
                self._literal.setdefault(f"{method} {_without_trailing_slash(setting['route'])}", []).append(setting)

    def by_template(self, method: str, template: str) -> RouteSetting | None:
        """At the end of a request: the "method + route template" the framework reports"""
        if not self._exact:
            return None
        return self._exact.get(f"{method} {template}") or self._exact.get(f"* {template}")

    def candidates_by_path(self, method: str, path: str) -> list[RouteSetting]:
        """At the start of a request: every template the path (without query string) matches, only to decide the reservation. There can be several (/users/me and /users/{id});
        which one the framework picks is only known afterwards, so the caller reserves for the largest: better to reserve too much and refund at the end than too little and let requests through over the limit.
        Starlette, Flask and Django all route case-sensitively, and so does this: matching case-insensitively would make the cheaper of two routes that differ only in case
        reserve like the expensive one, wrongly blocking customers close to their quota. One trailing slash is optional: being that much broader is fine, as hardly anyone prices
        two routes that differ only by a trailing slash differently"""
        found: list[RouteSetting] = []
        direct = self.by_template(method, path)
        if direct is not None:
            found.append(direct)
        literal_path = _without_trailing_slash(path)
        for key in (f"{method} {literal_path}", f"* {literal_path}"):
            found.extend(setting for setting in self._literal.get(key, ()) if setting is not direct)
        for pattern in self._patterns:
            if pattern.setting is not direct and pattern.method in (method, "*") and pattern.regex.match(path):
                found.append(pattern.setting)
        return found


def _normalize_method(method: str) -> str:
    upper = method.upper()
    return "*" if upper == "ALL" else upper


def _without_trailing_slash(path: str) -> str:
    return path[:-1] if len(path) > 1 and path.endswith("/") else path


def _has_parameters(template: str) -> bool:
    return any(character in template for character in ":*{<")


def _segments(template: str) -> list[str]:
    # A trailing slash on the template is treated as optional, like one on the path. Empty segments in the middle (/v1//users) are part of the template
    # and kept: dropping them would miss /v1//users/7, which the framework does route here, and reserve only 1 (same as the Node SDK's routes.ts)
    segments = template.split("/")[1:]
    if len(segments) > 1 and segments[-1] == "":
        segments.pop()
    return segments


def _template_to_regex(template: str) -> re.Pattern[str]:
    """Supports the usual forms: /v1/users/:id, optional /:id?, a trailing wildcard *, {id}, {rest:path}, <int:id>, <path:rest>.
    Every parameter matches "any characters within one segment" and path converters match "any number of segments", ignoring converters like int: matching too broadly only reserves more"""
    source = []
    for segment in _segments(template):
        if segment.startswith("*"):
            source.append("(?:/.*)?")
        elif segment.startswith(":"):
            source.append("(?:/[^/]+)?" if segment.endswith("?") else "/[^/]+")
        else:
            source.append("/" + _segment_regex(segment))
    return re.compile("^" + "".join(source) + "/?$")


def _segment_regex(segment: str) -> str:
    parts = []
    last = 0
    for match in _PARAMETER.finditer(segment):
        parts.append(re.escape(segment[last : match.start()]))
        converter = match.group("brace_converter") or match.group("angle_converter") or ""
        parts.append(".*" if converter.split("(")[0].strip() == "path" else "[^/]+")
        last = match.end()
    parts.append(re.escape(segment[last:]))
    return "".join(parts)
