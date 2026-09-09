#!/usr/bin/env python3
"""Rasterize the Astriena mark (SVG is the source of truth) to PNG. Stdlib only."""

from __future__ import annotations

import math
import struct
import zlib
from pathlib import Path

BG = (0x0B, 0x12, 0x20)
SURFACE = (0x14, 0x1C, 0x2E)
STAR = (0xE8, 0xD5, 0xA3)
CORE = (0xF7, 0xF1, 0xE1)
LINE = (0xC4, 0xB4, 0x8A)
DIM = (0x5C, 0x6B, 0x84)

# Keep these coordinates in sync with astriena-mark.svg's 64-unit viewBox.
STARS = [
    (18, 16, 2.25), (39, 31, 1.95), (25, 45, 1.72), (54, 27, 1.52),
    (43, 12, 1.34), (14, 34, 1.18), (31, 19, 1.03), (18, 54, .94),
    (9, 21, .82), (48, 46, .76), (34, 42, .66), (52, 16, .58),
]
LINES = [
    ((9, 21), (18, 16)), ((18, 16), (31, 19)), ((31, 19), (43, 12)),
    ((31, 19), (39, 31)), ((39, 31), (54, 27)),
    ((18, 16), (14, 34)), ((14, 34), (25, 45)), ((25, 45), (18, 54)),
]
FIELD = [
    (6, 9, .28), (14, 6, .38), (25, 8, .24), (36, 5, .31),
    (49, 7, .25), (58, 12, .36), (4, 26, .23), (8, 39, .34),
    (5, 51, .25), (13, 58, .30), (29, 58, .24), (41, 60, .33),
    (55, 56, .27), (60, 45, .35), (58, 35, .22), (61, 22, .29),
    (12, 27, .22), (22, 28, .30), (34, 27, .21), (47, 20, .24),
    (50, 38, .32), (44, 50, .22), (31, 51, .27), (11, 46, .20),
    (27, 14, .19), (39, 38, .24), (54, 15, .18), (35, 56, .18),
]

STARS_16 = [
    (4.6, 4, 1.18), (9.8, 7.8, 1.02), (6.2, 11.3, .92),
    (13.4, 6.8, .78), (10.8, 3, .67), (3.6, 8.5, .62),
    (12, 12.5, .48), (2.6, 5.2, .45),
]
LINES_16 = [
    ((2.6, 5.2), (4.6, 4)), ((4.6, 4), (7.8, 4.8)),
    ((9.8, 7.8), (13.4, 6.8)), ((3.6, 8.5), (6.2, 11.3)),
]


def lerp(a: int, b: int, t: float) -> int:
    return int(a + (b - a) * t)


def mix(c1: tuple[int, int, int], c2: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    t = max(0.0, min(1.0, t))
    return (lerp(c1[0], c2[0], t), lerp(c1[1], c2[1], t), lerp(c1[2], c2[2], t))


def blend(dst: list[int], i: int, color: tuple[int, int, int], a: float) -> None:
    if a <= 0:
        return
    if a >= 1:
        dst[i], dst[i + 1], dst[i + 2] = color
        return
    ia = 1.0 - a
    dst[i] = int(dst[i] * ia + color[0] * a)
    dst[i + 1] = int(dst[i + 1] * ia + color[1] * a)
    dst[i + 2] = int(dst[i + 2] * ia + color[2] * a)


class Canvas:
    def __init__(self, w: int, h: int, bg: tuple[int, int, int] = BG) -> None:
        self.w = w
        self.h = h
        self.px = [0] * (w * h * 3)
        for i in range(0, len(self.px), 3):
            self.px[i], self.px[i + 1], self.px[i + 2] = bg

    def _idx(self, x: int, y: int) -> int | None:
        if 0 <= x < self.w and 0 <= y < self.h:
            return (y * self.w + x) * 3
        return None

    def set(self, x: int, y: int, color: tuple[int, int, int], a: float = 1.0) -> None:
        i = self._idx(x, y)
        if i is not None:
            blend(self.px, i, color, a)

    def fill_radial(self, cx: float, cy: float, radius: float, inner: tuple[int, int, int], outer: tuple[int, int, int]) -> None:
        for y in range(self.h):
            for x in range(self.w):
                t = math.hypot(x - cx, y - cy) / radius
                c = mix(inner, outer, min(t, 1.0))
                i = (y * self.w + x) * 3
                self.px[i], self.px[i + 1], self.px[i + 2] = c

    def rounded_rect_mask(self, radius: float) -> None:
        r = radius
        for y in range(self.h):
            for x in range(self.w):
                cx = r if x < r else (self.w - 1 - r if x > self.w - 1 - r else x)
                cy = r if y < r else (self.h - 1 - r if y > self.h - 1 - r else y)
                if x < r or x > self.w - 1 - r or y < r or y > self.h - 1 - r:
                    if math.hypot(x - cx, y - cy) > r:
                        self.set(x, y, BG, 1.0)

    def circle(self, cx: float, cy: float, r: float, color: tuple[int, int, int], a: float = 1.0) -> None:
        x0, x1 = int(cx - r - 1), int(cx + r + 2)
        y0, y1 = int(cy - r - 1), int(cy + r + 2)
        for y in range(y0, y1):
            for x in range(x0, x1):
                d = math.hypot(x + 0.5 - cx, y + 0.5 - cy)
                cov = max(0.0, min(1.0, r + 0.5 - d))
                if cov:
                    self.set(x, y, color, a * cov)

    def line(
        self, x0: float, y0: float, x1: float, y1: float, width: float,
        color: tuple[int, int, int], alpha: float = .22,
    ) -> None:
        steps = max(2, int(math.hypot(x1 - x0, y1 - y0) * 2))
        r = max(.35, width / 2.0)
        for i in range(steps + 1):
            t = i / steps
            self.circle(x0 + (x1 - x0) * t, y0 + (y1 - y0) * t, r, color, alpha)

    def star(self, cx: float, cy: float, r: float) -> None:
        if r >= 1.3:
            self.circle(cx, cy, r * 2.1, STAR, .08)
        self.circle(cx, cy, r, STAR, 1.0)
        self.circle(cx, cy, max(.34, r * .30), CORE, 1.0)


def png(canvas: Canvas, path: Path) -> None:
    raw = b"".join(b"\x00" + bytes(canvas.px[y * canvas.w * 3 : (y + 1) * canvas.w * 3]) for y in range(canvas.h))

    def chunk(tag: bytes, data: bytes) -> bytes:
        crc = zlib.crc32(tag + data) & 0xFFFFFFFF
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", canvas.w, canvas.h, 8, 2, 0, 0, 0)
    path.write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", ihdr)
        + chunk(b"IDAT", zlib.compress(raw, 9))
        + chunk(b"IEND", b"")
    )


def draw_asterism(
    c: Canvas, ox: float, oy: float, scale: float, line_w: float,
    line_alpha: float = .22,
) -> None:
    for (x0, y0), (x1, y1) in LINES:
        c.line(
            ox + x0 * scale, oy + y0 * scale,
            ox + x1 * scale, oy + y1 * scale,
            line_w, LINE, line_alpha,
        )
    for x, y, r in STARS:
        c.star(ox + x * scale, oy + y * scale, r * scale)


def render_mark(size: int, path: Path) -> None:
    c = Canvas(size, size, BG)
    c.fill_radial(size * .34, size * .27, size * .82, SURFACE, BG)
    c.rounded_rect_mask(size * 14 / 64)
    scale = size / 64.0
    for x, y, r in FIELD:
        if size <= 64 and r < .24:
            continue
        c.circle(x * scale, y * scale, max(.55, r * scale), DIM, .72)
    draw_asterism(c, 0, 0, scale, .36 * scale)
    png(c, path)


def render_mark_16(path: Path) -> None:
    c = Canvas(16, 16, BG)
    c.rounded_rect_mask(3.5)
    for (x0, y0), (x1, y1) in LINES_16:
        c.line(x0, y0, x1, y1, .58, LINE, .38)
    for x, y, r in STARS_16:
        c.circle(x, y, r, STAR)
    c.circle(4.6, 4, .35, CORE)
    c.circle(9.8, 7.8, .30, CORE)
    png(c, path)


def render_social(path: Path) -> None:
    w, h = 1280, 640
    c = Canvas(w, h, BG)
    c.fill_radial(w * 0.32, h * 0.40, w * 0.80, SURFACE, BG)
    field = [
        (72, 64, 1.1), (148, 128, .8), (228, 42, 1), (400, 86, .7),
        (968, 62, 1.1), (1092, 118, .85), (1176, 196, 1), (1032, 516, .9),
        (188, 536, 1), (54, 392, .75), (708, 74, .65), (852, 552, .85),
        (1232, 388, .75), (492, 576, .65), (620, 40, .7), (780, 520, .8),
        (1140, 48, .6), (320, 600, .7), (40, 220, .65), (1260, 280, .7),
        (880, 96, .55), (560, 600, .6), (980, 300, .7), (160, 300, .5),
    ]
    for x, y, r in field:
        c.circle(x, y, r, DIM, 1.0)
    scale = 7.2
    draw_asterism(c, 640 - 32 * scale, 320 - 31 * scale, scale, .36 * scale)
    png(c, path)


def main() -> None:
    here = Path(__file__).resolve().parent
    render_mark(512, here / "astriena-icon-512.png")
    render_mark(64, here / "astriena-icon-64.png")
    render_mark_16(here / "astriena-icon-16.png")
    render_social(here / "astriena-social.png")


if __name__ == "__main__":
    main()
