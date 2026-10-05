"""Render the app icon (1024px PNG) for `cargo tauri icon`."""
import sys
from PIL import Image, ImageDraw

S = 1024
img = Image.new("RGBA", (S, S), (0, 0, 0, 0))
d = ImageDraw.Draw(img)
d.rounded_rectangle((64, 64, S - 64, S - 64), radius=200, fill=(10, 14, 22, 255))
WHITE, BLUE, GREEN, RED = (244, 247, 251, 255), (61, 155, 255, 255), (46, 229, 157, 255), (255, 77, 94, 255)
# three worker bays
for i, busy in enumerate([True, False, True]):
    x0 = 190 + i * 230
    d.rounded_rectangle((x0, 250, x0 + 190, 500), radius=34, outline=GREEN if busy else WHITE, width=14)
    if busy:
        d.ellipse((x0 + 65, 345, x0 + 125, 405), fill=GREEN)
# snapshot store
d.line((190, 610, 834, 610), fill=(244, 247, 251, 90), width=8)
for r in range(2):
    for c in range(9):
        x, y = 205 + c * 72, 660 + r * 72
        d.ellipse((x, y, x + 40, y + 40), fill=RED if (r, c) == (1, 7) else BLUE)
img.save(sys.argv[1] if len(sys.argv) > 1 else "icon-src.png")
