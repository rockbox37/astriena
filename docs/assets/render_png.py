#!/usr/bin/env python3
"""Rasterize the Astriena mark (SVG is the source of truth) to PNG. Stdlib only."""

from __future__ import annotations

import math
import struct
import zlib
from pathlib import Path

BG = (0x0B, 0x12, 0x20)
SURFACE = (0x1A, 0x27, 0x40)
STAR = (0xE8, 0xD5, 0xA3)
CORE = (0xF7, 0xF1, 0xE1)
LINE = (0xC4, 0xB4, 0x8A)
DIM = (0x5C, 0x6B, 0x84)

STARS = [(32, 11, 2.05), (19, 26, 1.55), (45, 24, 1.55), (15, 50, 1.35), (50, 51, 1.35)]
LINES = [((32, 11), (19, 26)), ((19, 26), (15, 50)), ((32, 11), (45, 24)), ((45, 24), (50, 51)), ((19, 26), (45, 24))]
FIELD = [(10, 16, 0.55), (56, 12, 0.45), (58, 42, 0.5), (22, 58, 0.4), (40, 7, 0.45)]


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

    def line(self, x0: float, y0: float, x1: float, y1: float, width: float, color: tuple[int, int, int]) -> None:
        steps = max(2, int(math.hypot(x1 - x0, y1 - y0) * 2))
        r = max(0.6, width / 2.0)
        for i in range(steps + 1):
            t = i / steps
            self.circle(x0 + (x1 - x0) * t, y0 + (y1 - y0) * t, r, color, 1.0)

    def star(self, cx: float, cy: float, r: float) -> None:
        self.circle(cx, cy, r, STAR, 1.0)
        self.circle(cx, cy, max(0.6, r * 0.32), CORE, 1.0)


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


def draw_asterism(c: Canvas, ox: float, oy: float, scale: float, line_w: float) -> None:
    for (x0, y0), (x1, y1) in LINES:
        c.line(ox + x0 * scale, oy + y0 * scale, ox + x1 * scale, oy + y1 * scale, line_w, LINE)
    for x, y, r in STARS:
        c.star(ox + x * scale, oy + y * scale, r * scale)


def render_mark(size: int, path: Path) -> None:
    c = Canvas(size, size, BG)
    c.fill_radial(size * 0.38, size * 0.28, size * 0.78, SURFACE, BG)
    c.rounded_rect_mask(size * 14 / 64)
    scale = size / 64.0
    for x, y, r in FIELD:
        c.circle(x * scale, y * scale, max(0.8, r * scale), DIM, 1.0)
    draw_asterism(c, 0, 0, scale, 1.7 * scale)
    png(c, path)


def render_social(path: Path) -> None:
    w, h = 1280, 640
    c = Canvas(w, h, BG)
    c.fill_radial(w * 0.32, h * 0.40, w * 0.80, SURFACE, BG)
    field = [
        (80, 70, 1.2), (160, 140, 0.9), (240, 48, 1.1), (420, 90, 0.8),
        (980, 70, 1.2), (1100, 120, 0.9), (1180, 200, 1.1), (1040, 520, 1.0),
        (200, 540, 1.1), (60, 400, 0.8), (720, 80, 0.7), (860, 560, 0.9),
        (1240, 400, 0.8), (500, 580, 0.7),
    ]
    for x, y, r in field:
        c.circle(x, y, r, DIM, 1.0)
    scale = 7.2
    draw_asterism(c, 640 - 32 * scale, 320 - 31 * scale, scale, 1.7 * scale)
    png(c, path)


def main() -> None:
    here = Path(__file__).resolve().parent
    render_mark(512, here / "astriena-icon-512.png")
    render_mark(64, here / "astriena-icon-64.png")
    render_social(here / "astriena-social.png")


if __name__ == "__main__":
    main()
