"""Draw the README architecture diagram as two SVGs (light and dark GitHub themes).

Run from the repo root:  python3 docs/images/architecture.py
"""

from pathlib import Path

W, H = 1240, 720

THEMES = {
    "light": dict(
        bg="#ffffff", text="#1f2328", muted="#59636e", frame="#8c959f",
        vm_fill="#f6f8fa", vm_stroke="#d0d7de",
        kafka_fill="#ddf4ff", kafka_stroke="#0969da", leader="#0969da", leader_text="#ffffff",
        replica_fill="#ffffff", band="#b6e3ff",
        client_fill="#dafbe1", client_stroke="#1a7f37",
        spark_fill="#fff1e5", spark_stroke="#bc4c00",
        file_fill="#ffffff", file_stroke="#8c959f",
        arrow="#1f2328", repl="#0969da", fail="#cf222e", step_fill="#1f2328", step_text="#ffffff",
    ),
    "dark": dict(
        bg="#0d1117", text="#e6edf3", muted="#9198a1", frame="#656c76",
        vm_fill="#151b23", vm_stroke="#3d444d",
        kafka_fill="#0c2d6b", kafka_stroke="#4493f8", leader="#1f6feb", leader_text="#ffffff",
        replica_fill="#0d1117", band="#1158c7",
        client_fill="#0f2e1a", client_stroke="#3fb950",
        spark_fill="#3d1d00", spark_stroke="#f0883e",
        file_fill="#0d1117", file_stroke="#656c76",
        arrow="#e6edf3", repl="#4493f8", fail="#f85149", step_fill="#e6edf3", step_text="#0d1117",
    ),
}

SANS = "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Noto Sans', Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, 'Liberation Mono', monospace"


def draw(t):
    out = []
    a = out.append

    def text(x, y, s, size=12, color=None, weight=400, anchor="middle", mono=False):
        a(f'<text x="{x}" y="{y}" font-family="{MONO if mono else SANS}" font-size="{size}" '
          f'font-weight="{weight}" fill="{color or t["text"]}" text-anchor="{anchor}">{s}</text>')

    def box(x, y, w, h, fill, stroke, rx=8, dash=None, sw=1.5):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}" '
          f'stroke="{stroke}" stroke-width="{sw}"{d}/>')

    def node(x, y, w, h, kind, label, sub=None):
        fill, stroke = {"client": (t["client_fill"], t["client_stroke"]),
                        "spark": (t["spark_fill"], t["spark_stroke"])}[kind]
        box(x, y, w, h, fill, stroke)
        cy = y + h / 2
        if sub:
            text(x + w / 2, cy - 2, label, 13, weight=600)
            text(x + w / 2, cy + 14, sub, 10.5, t["muted"])
        else:
            text(x + w / 2, cy + 4.5, label, 13, weight=600)

    def file(x, y, w, h, name):
        f = 12  # folded corner
        a(f'<path d="M{x} {y} H{x + w - f} L{x + w} {y + f} V{y + h} H{x} Z" fill="{t["file_fill"]}" '
          f'stroke="{t["file_stroke"]}" stroke-width="1.3"/>')
        a(f'<path d="M{x + w - f} {y} V{y + f} H{x + w}" fill="none" stroke="{t["file_stroke"]}" stroke-width="1.3"/>')
        text(x + w / 2, y + h / 2 + 4, name, 11, mono=True)

    def arrow(points, color=None, dash=None, marker="arr", sw=1.8):
        color = color or t["arrow"]
        d = f' stroke-dasharray="{dash}"' if dash else ""
        pts = " ".join(f"{px},{py}" for px, py in points)
        a(f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="{sw}" '
          f'stroke-linejoin="round"{d} marker-end="url(#{marker})"/>')

    def step(x, y, n):
        a(f'<circle cx="{x}" cy="{y}" r="10" fill="{t["step_fill"]}"/>')
        text(x, y + 4, str(n), 11, t["step_text"], 700)

    def label(x, y, s, anchor="middle"):
        text(x, y, s, 10.5, t["muted"], anchor=anchor)

    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
      f'aria-label="Orders flow from VM1 through the orders topic to VM4, where Spark Job 1 enriches them; the '
      f'metrics flow back through the order_metrics topic to VM1 for Spark Job 2. Both topics have two partitions '
      f'replicated across the brokers on VM2 and VM3.">')
    a("<defs>")
    for mid, col in (("arr", t["arrow"]), ("rep", t["repl"]), ("fail", t["fail"])):
        a(f'<marker id="{mid}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
          f'orient="auto-start-reverse"><path d="M0 0 L10 5 L0 10 z" fill="{col}"/></marker>')
    a("</defs>")
    box(0, 0, W, H, t["bg"], t["bg"], rx=14, sw=0)

    # ZeroTier boundary
    box(14, 30, W - 28, H - 44, "none", t["frame"], rx=16, dash="6 5", sw=1.2)
    a(f'<rect x="34" y="20" width="236" height="22" rx="11" fill="{t["bg"]}"/>')
    text(44, 36, "ZeroTier private network", 12.5, t["muted"], 600, anchor="start")

    # ---- machine columns
    VM1X, KX, VM4X, COLY, COLH = 34, 418, 870, 58, 572
    box(VM1X, COLY, 340, COLH, t["vm_fill"], t["vm_stroke"], rx=12)
    box(VM4X, COLY, 336, COLH, t["vm_fill"], t["vm_stroke"], rx=12)
    text(VM1X + 170, COLY + 26, "VM1 · Dhriti", 15, weight=700)
    text(VM1X + 170, COLY + 44, "ingest and final analytics", 11, t["muted"])
    text(VM4X + 168, COLY + 26, "VM4 · Navya", 15, weight=700)
    text(VM4X + 168, COLY + 44, "enrichment and relay", 11, t["muted"])

    # ---- Kafka cluster
    box(KX, COLY, 412, COLH, t["kafka_fill"], t["kafka_stroke"], rx=12, sw=1.8)
    text(KX + 206, COLY + 26, "Kafka 3.9.2 cluster (KRaft)", 15, weight=700)
    text(KX + 206, COLY + 44, "2 partitions per topic · replication factor 2 · min.insync.replicas 1", 11, t["muted"])

    B2X, B3X, BW, BY, BH = KX + 22, KX + 216, 174, COLY + 60, 500
    box(B2X, BY, BW, BH, t["bg"], t["kafka_stroke"], rx=10, sw=1.2)
    box(B3X, BY, BW, BH, t["bg"], t["fail"], rx=10, sw=1.4, dash="5 4")
    text(B2X + BW / 2, BY + 22, "VM2 · Vivian", 13, weight=700)
    text(B2X + BW / 2, BY + 38, "broker 1 + controller", 10.5, t["muted"])
    text(B3X + BW / 2, BY + 22, "VM3 · Dhanya", 13, weight=700)
    text(B3X + BW / 2, BY + 38, "broker 2", 10.5, t["muted"])

    def topic(y, name):
        a(f'<rect x="{KX + 8}" y="{y}" width="396" height="92" rx="8" fill="{t["band"]}" fill-opacity="0.35"/>')
        text(KX + 206, y + 17, f"topic: {name}", 12, weight=700, mono=True)
        rows = (("p0", B2X, B3X), ("p1", B3X, B2X))
        for i, (p, lead_x, rep_x) in enumerate(rows):
            cy = y + 32 + i * 30
            a(f'<rect x="{lead_x + 22}" y="{cy}" width="{BW - 44}" height="22" rx="11" fill="{t["leader"]}"/>')
            text(lead_x + BW / 2, cy + 15.5, f"{p} leader", 11.5, t["leader_text"], 600)
            a(f'<rect x="{rep_x + 22}" y="{cy}" width="{BW - 44}" height="22" rx="11" fill="{t["replica_fill"]}" '
              f'stroke="{t["leader"]}" stroke-width="1.4"/>')
            text(rep_x + BW / 2, cy + 15.5, f"{p} replica", 11.5, t["text"], 600)
            if lead_x < rep_x:
                arrow([(lead_x + BW - 22, cy + 11), (rep_x + 24, cy + 11)], t["repl"], "4 3", "rep", 1.4)
            else:
                arrow([(lead_x + 22, cy + 11), (rep_x + BW - 24, cy + 11)], t["repl"], "4 3", "rep", 1.4)

    ORD_Y, MET_Y = 166, 416
    topic(ORD_Y, "orders")
    topic(MET_Y, "order_metrics")

    # KRaft controller lives on VM2; broker 2 registers with it
    cy = 300
    a(f'<rect x="{B2X + 18}" y="{cy}" width="{BW - 36}" height="28" rx="6" fill="{t["bg"]}" '
      f'stroke="{t["kafka_stroke"]}" stroke-width="1.4"/>')
    text(B2X + BW / 2, cy + 18.5, "KRaft controller", 11.5, weight=600)
    arrow([(B3X + 40, cy + 14), (B2X + BW - 16, cy + 14)], t["muted"], "2 3", "arr", 1.4)
    label(B3X + BW / 2 + 12, cy - 6, "registers + heartbeats")
    label(B3X + BW / 2 + 12, cy + 42, "controller port 9093")

    # failure test callout at the bottom of VM3
    text(B3X + BW / 2, BY + BH - 30, "stopped in the", 11, t["fail"], 600)
    text(B3X + BW / 2, BY + BH - 15, "failure test", 11, t["fail"], 600)
    text(B2X + BW / 2, BY + BH - 30, "takes over every", 11, t["muted"])
    text(B2X + BW / 2, BY + BH - 15, "partition if VM3 stops", 11, t["muted"])

    # ---- VM1 contents
    ORD_MID, MET_MID = ORD_Y + 46, MET_Y + 46
    file(VM1X + 16, ORD_MID - 18, 150, 36, "data/order.jsonl")
    node(VM1X + 196, ORD_MID - 26, 126, 52, "client", "Producer 1", "raw lines, unparsed")
    arrow([(VM1X + 166, ORD_MID), (VM1X + 194, ORD_MID)])
    arrow([(VM1X + 322, ORD_MID), (KX + 6, ORD_MID)])
    step(VM1X + 362, ORD_MID - 16, 1)

    node(VM1X + 196, MET_MID - 26, 126, 52, "client", "Consumer 2", "stops when idle")
    arrow([(KX + 6, MET_MID), (VM1X + 324, MET_MID)])
    step(VM1X + 362, MET_MID - 16, 5)
    file(VM1X + 16, MET_MID + 52, 222, 36, "order_metrics_landed.jsonl")
    arrow([(VM1X + 259, MET_MID + 26), (VM1X + 259, MET_MID + 70), (VM1X + 240, MET_MID + 70)])
    label(VM1X + 300, MET_MID + 48, "local file")
    node(VM1X + 16, MET_MID + 118, 120, 52, "spark", "Spark Job 2", "restaurant + city")
    arrow([(VM1X + 76, MET_MID + 88), (VM1X + 76, MET_MID + 116)])
    step(VM1X + 104, MET_MID + 102, 6)
    file(VM1X + 156, MET_MID + 112, 168, 28, "restaurant_summary.csv")
    file(VM1X + 156, MET_MID + 148, 168, 28, "city_summary.csv")
    arrow([(VM1X + 136, MET_MID + 136), (VM1X + 146, MET_MID + 136), (VM1X + 146, MET_MID + 126), (VM1X + 154, MET_MID + 126)])
    arrow([(VM1X + 146, MET_MID + 136), (VM1X + 146, MET_MID + 162), (VM1X + 154, MET_MID + 162)])

    # ---- VM4 contents
    node(VM4X + 18, ORD_MID - 26, 126, 52, "client", "Consumer 1", "stops when idle")
    arrow([(KX + 412 - 6, ORD_MID), (VM4X + 16, ORD_MID)])
    step(VM4X - 20, ORD_MID - 16, 2)
    file(VM4X + 168, ORD_MID - 18, 152, 36, "orders_landed.jsonl")
    arrow([(VM4X + 144, ORD_MID), (VM4X + 166, ORD_MID)])
    node(VM4X + 168, ORD_MID + 52, 152, 52, "spark", "Spark Job 1", "clean + 7 metrics")
    arrow([(VM4X + 244, ORD_MID + 18), (VM4X + 244, ORD_MID + 50)])
    step(VM4X + 150, ORD_MID + 78, 3)
    label(VM4X + 278, ORD_MID + 38, "local file")
    file(VM4X + 168, ORD_MID + 134, 152, 36, "order_metrics.jsonl")
    arrow([(VM4X + 244, ORD_MID + 104), (VM4X + 244, ORD_MID + 132)])
    node(VM4X + 18, MET_MID - 26, 126, 52, "client", "Producer 2", "one record per line")
    arrow([(VM4X + 244, ORD_MID + 170), (VM4X + 244, MET_MID), (VM4X + 146, MET_MID)])
    arrow([(VM4X + 18, MET_MID), (KX + 412 + 6 - 12, MET_MID)])
    step(VM4X - 20, MET_MID - 16, 4)

    # legend + rule reminder under the columns
    LY = COLY + COLH + 28
    lx = 60
    a(f'<rect x="{lx}" y="{LY - 9}" width="44" height="18" rx="9" fill="{t["leader"]}"/>')
    text(lx + 52, LY + 4, "partition leader", 11, t["muted"], anchor="start")
    lx += 170
    a(f'<rect x="{lx}" y="{LY - 9}" width="44" height="18" rx="9" fill="{t["replica_fill"]}" stroke="{t["leader"]}" stroke-width="1.4"/>')
    text(lx + 52, LY + 4, "follower replica", 11, t["muted"], anchor="start")
    lx += 175
    arrow([(lx, LY), (lx + 44, LY)], t["repl"], "4 3", "rep", 1.4)
    text(lx + 52, LY + 4, "leader → follower replication", 11, t["muted"], anchor="start")
    lx += 245
    step(lx + 10, LY, 1)
    text(lx + 28, LY + 4, "pipeline order", 11, t["muted"], anchor="start")
    lx += 140
    a(f'<path d="M{lx} {LY - 9} H{lx + 34} L{lx + 42} {LY - 1} V{LY + 9} H{lx} Z" fill="{t["file_fill"]}" stroke="{t["file_stroke"]}" stroke-width="1.3"/>')
    text(lx + 50, LY + 4, "local file (Spark never reads Kafka directly)", 11, t["muted"], anchor="start")
    label(W / 2, LY + 30, "no file is copied from VM4 to VM1: the metrics reach VM1 only through the order_metrics topic")

    a("</svg>")
    return "\n".join(out)


if __name__ == "__main__":
    here = Path(__file__).resolve().parent
    for name, theme in THEMES.items():
        (here / f"architecture-{name}.svg").write_text(draw(theme) + "\n", encoding="utf-8")
        print(here / f"architecture-{name}.svg")
