"""5パターン三連複フォーメーション比較バックテスト
EV上位15点に制限し、実際の三連複配当で回収率を計算する。
"""
import json, sys, io
from pathlib import Path
import statistics

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

pred_dir = Path("data/predictions")
res_dir = Path("data/results")
pred_files = sorted(pred_dir.glob("2026*_pred.json"))

MAX_TICKETS = 15  # 点数上限


def detect_clusters(comps, threshold):
    if not comps:
        return []
    clusters = [[comps[0]]]
    for i in range(1, len(comps)):
        if comps[i - 1] - comps[i] > threshold:
            clusters.append([])
        clusters[-1].append(comps[i])
    return clusters


def build_p1(horses, _conf="B"):
    """パターン1: クラスター断層法"""
    safe = sorted(
        [h for h in horses if not h.get("is_tokusen_kiken")],
        key=lambda h: -h.get("composite", 0),
    )
    comps = [h.get("composite", 0) for h in safe]
    if len(comps) < 3:
        return [], [], []
    sigma = statistics.stdev(comps) if len(comps) > 1 else 5.0
    threshold = max(2.0, sigma * 0.5)
    cls = detect_clusters(comps, threshold)
    if len(cls) < 2:
        return safe[:2], safe[:4], safe[:6]
    n1 = min(len(cls[0]), 3)
    n2 = min(n1 + (len(cls[1]) if len(cls) > 1 else 0), 6)
    n3 = min(n2 + (len(cls[2]) if len(cls) > 2 else 2), 10)
    return safe[:n1], safe[:n2], safe[:n3]


def build_p2(horses, _conf="B"):
    """パターン2: 印階層 + 指数差フィルター法"""
    safe = sorted(
        [h for h in horses if not h.get("is_tokusen_kiken")],
        key=lambda h: -h.get("composite", 0),
    )
    if not safe:
        return [], [], []
    tc = safe[0].get("composite", 50)
    c1 = [
        h for h in safe
        if h.get("mark", "") in ("◉", "◎") or (tc - h.get("composite", 0)) <= 3.0
    ][:3]
    c2 = [
        h for h in safe
        if h in c1 or h.get("mark", "") in ("○", "▲") or (tc - h.get("composite", 0)) <= 8.0
    ][:6]
    c3 = [
        h for h in safe
        if h in c2 or h.get("mark", "") in ("△", "★", "☆") or (tc - h.get("composite", 0)) <= 15.0
    ][:10]
    if not c1:
        c1 = safe[:1]
    return c1, c2, c3


def build_p3(horses, _conf="B"):
    """パターン3: 複勝率×期待値ハイブリッド法"""
    safe = sorted(
        [h for h in horses if not h.get("is_tokusen_kiken")],
        key=lambda h: -(h.get("place3_prob", 0) or 0),
    )
    if not safe:
        return [], [], []
    c1 = [h for h in safe if (h.get("place3_prob", 0) or 0) >= 0.30][:3]
    c2 = [h for h in safe if (h.get("place3_prob", 0) or 0) >= 0.15][:6]
    c3 = list(c2)
    for h in safe:
        if h in c3:
            continue
        if len(c3) >= 10:
            break
        odds = h.get("odds", 0) or h.get("predicted_tansho_odds", 0) or 10
        wp = h.get("win_prob", 0) or 0
        if wp * odds >= 1.5 or (h.get("place3_prob", 0) or 0) >= 0.10:
            c3.append(h)
    if not c1:
        c1 = safe[:2]
    if not c2:
        c2 = safe[:4]
    return c1, c2, c3[:10]


def build_p4(horses, conf="B"):
    """パターン4: 自信度アダプティブ法"""
    safe = sorted(
        [h for h in horses if not h.get("is_tokusen_kiken")],
        key=lambda h: -h.get("composite", 0),
    )
    if not safe:
        return [], [], []
    tc = safe[0].get("composite", 50)
    # SS/S: 三連複は3頭必要→間隔を十分に確保
    params = {
        "SS": (4, 10, 16), "S": (4, 9, 14), "A": (4, 8, 12),
        "B": (5, 10, 15), "C": (5, 12, 18), "D": (3, 6, 8), "E": (3, 6, 8),
    }
    g1, g2, g3 = params.get(conf, (4, 8, 12))
    c1 = [h for h in safe if (tc - h.get("composite", 0)) <= g1][:3]
    c2 = [h for h in safe if (tc - h.get("composite", 0)) <= g2][:6]
    c3 = [h for h in safe if (tc - h.get("composite", 0)) <= g3][:10]
    # 最低頭数フォールバック（三連複に3頭必須）
    if len(c1) < 1: c1 = safe[:1]
    if len(c2) < 2: c2 = safe[:2]
    if len(c3) < 3: c3 = safe[:3]
    return c1, c2, c3


def build_p5(horses, conf="B"):
    """パターン5: 断層+印+自信度の統合法"""
    safe = sorted(
        [h for h in horses if not h.get("is_tokusen_kiken")],
        key=lambda h: -h.get("composite", 0),
    )
    comps = [h.get("composite", 0) for h in safe]
    if len(comps) < 3:
        return [], [], []
    sigma = statistics.stdev(comps) if len(comps) > 1 else 5.0
    threshold = max(2.0, sigma * 0.5)
    cls = detect_clusters(comps, threshold)
    # SS/S: 間隔を広めに取り、三連複に必要な頭数を確保
    params = {
        "SS": (10, 18, 2, 5, 8), "S": (9, 16, 2, 5, 8),
        "A": (8, 14, 2, 5, 9), "B": (10, 16, 2, 6, 10),
        "C": (12, 18, 2, 6, 10), "D": (6, 8, 2, 4, 6), "E": (6, 8, 2, 4, 6),
    }
    g2, g3, cap1, cap2, cap3 = params.get(conf, (8, 14, 2, 5, 9))
    tc = comps[0] if comps else 50

    n1 = len(cls[0]) if cls else 2
    c1_idx = set(range(min(n1, cap1)))
    for i, h in enumerate(safe):
        if h.get("mark", "") in ("◉", "◎"):
            c1_idx.add(i)
    c1 = [safe[i] for i in sorted(c1_idx) if i < len(safe)][:cap1]

    c2_ids = {id(h) for h in c1}
    n2 = n1 + (len(cls[1]) if len(cls) > 1 else 0)
    for i, h in enumerate(safe):
        if i < n2 or h.get("mark", "") in ("○", "▲"):
            if (tc - h.get("composite", 0)) <= g2:
                c2_ids.add(id(h))
    c2 = [h for h in safe if id(h) in c2_ids][:cap2]

    c3_ids = {id(h) for h in c2}
    # SS/S は EV フィルターを緩和（place3_prob のみでフィルタ）
    high_conf = conf in ("SS", "S")
    for h in safe:
        if id(h) in c3_ids:
            continue
        if (tc - h.get("composite", 0)) > g3:
            continue
        p3 = h.get("place3_prob", 0) or 0
        if p3 < 0.08:
            continue
        if not high_conf:
            odds = h.get("odds", 0) or h.get("predicted_tansho_odds", 0) or 10
            wp = h.get("win_prob", 0) or 0
            if wp * odds < 0.8:
                continue
        c3_ids.add(id(h))
    c3 = [h for h in safe if id(h) in c3_ids][:cap3]

    # 最低頭数フォールバック（三連複に3頭必須）
    if len(c1) < 1: c1 = safe[:1]
    if len(c2) < 2: c2 = safe[:2]
    if len(c3) < 3: c3 = safe[:3]
    return c1, c2, c3


def gen_tickets_with_ev(c1, c2, c3, hmap, fc, is_jra):
    """フォーメーション全組合せをEV付きで生成し、EV降順ソート"""
    seen = set()
    tickets = []
    for a in c1:
        for b in c2:
            for c in c3:
                nos = frozenset([a.get("horse_no"), b.get("horse_no"), c.get("horse_no")])
                if len(nos) == 3 and nos not in seen:
                    seen.add(nos)
                    # EV = prob * odds
                    probs = []
                    odds_list = []
                    for no in nos:
                        h = hmap.get(no, {})
                        probs.append(h.get("place3_prob", 0) or 0.1)
                        odds_list.append(max(h.get("odds", 0) or h.get("predicted_tansho_odds", 0) or 10, 1.0))
                    # 三連複確率近似
                    n = fc
                    corr = n * (n - 1) / max(1.0, (n - 2) * (n - 3) * 0.5)
                    prob = min(probs[0] * probs[1] * probs[2] * corr, 0.99)
                    # オッズ推定
                    if fc <= 8: f = 12.0
                    elif fc <= 10: f = 16.0
                    elif fc <= 12: f = 20.0
                    elif fc <= 14: f = 24.0
                    else: f = 28.0
                    pr = 0.750 / 0.800 if is_jra else 0.700 / 0.750
                    prod_o = 1.0
                    for o in odds_list:
                        prod_o *= o
                    est_odds = max(2.0, prod_o / f * pr)
                    ev = prob * est_odds
                    tickets.append({"nos": nos, "ev": ev})
    # EV降順
    tickets.sort(key=lambda t: -t["ev"])
    return tickets


def get_sanrenpuku_payout(payouts):
    """結果JSONから三連複払戻を取得"""
    for key in ("三連複", "3連複"):
        p = payouts.get(key, {})
        if isinstance(p, dict) and p.get("payout", 0) > 0:
            return p["payout"]
    san = payouts.get("sanrenpuku", [])
    if isinstance(san, list) and san:
        return san[0].get("payout", 0)
    elif isinstance(san, dict):
        return san.get("payout", 0)
    return 0


def main():
    builders = {
        1: build_p1, 2: build_p2, 3: build_p3, 4: build_p4, 5: build_p5,
    }
    stats = {}
    for p in range(1, 6):
        stats[p] = {"total": 0, "hits": 0, "tix": [], "inv": 0, "ret": 0, "bc": {}}

    total_races = 0

    for pf in pred_files:
        ds = pf.stem.replace("_pred", "")
        rf = res_dir / (ds + "_results.json")
        if not rf.exists():
            continue
        pred = json.loads(pf.read_text(encoding="utf-8"))
        results = json.loads(rf.read_text(encoding="utf-8"))

        for race in pred.get("races", []):
            rid = race.get("race_id", "")
            if rid not in results:
                continue
            res = results[rid]
            order = res.get("order", [])
            if not order:
                continue
            top3 = set()
            for o in order:
                if isinstance(o, dict) and o.get("finish", 99) <= 3:
                    top3.add(o["horse_no"])
            if len(top3) < 3:
                continue

            total_races += 1
            horses = race.get("horses", [])
            if len(horses) < 3:
                continue
            conf = race.get("confidence", "B")
            fc = race.get("field_count", len(horses))
            is_jra = race.get("is_jra", True)
            hmap = {h["horse_no"]: h for h in horses}
            actual_payout = get_sanrenpuku_payout(res.get("payouts", {}))
            top3_fs = frozenset(top3)

            for pn, bld in builders.items():
                c1, c2, c3 = bld(horses, conf)
                if not c1 or not c2 or not c3:
                    continue
                # EV付き全組合せ生成 → EV上位MAX_TICKETS点に制限
                all_tickets = gen_tickets_with_ev(c1, c2, c3, hmap, fc, is_jra)
                kept = all_tickets[:MAX_TICKETS]
                nt = len(kept)
                if nt == 0:
                    continue

                hit = any(t["nos"] == top3_fs for t in kept)
                inv = nt * 100
                ret = actual_payout if (hit and actual_payout > 0) else 0

                s = stats[pn]
                s["total"] += 1
                s["tix"].append(nt)
                s["inv"] += inv
                s["ret"] += ret
                if hit:
                    s["hits"] += 1

                if conf not in s["bc"]:
                    s["bc"][conf] = {"total": 0, "hits": 0, "tix": [], "inv": 0, "ret": 0}
                bc = s["bc"][conf]
                bc["total"] += 1
                bc["tix"].append(nt)
                bc["inv"] += inv
                bc["ret"] += ret
                if hit:
                    bc["hits"] += 1

    # 結果表示
    names = {
        1: "クラスター断層法",
        2: "印+指数差フィルター",
        3: "複勝率×EV",
        4: "自信度アダプティブ",
        5: "断層+印+自信度統合",
    }
    print("=" * 95)
    print(f"5パターン比較 (2026年 {total_races}R, 実際の三連複配当, EV上位{MAX_TICKETS}点制限)")
    print("=" * 95)
    for p in range(1, 6):
        s = stats[p]
        if s["total"] == 0:
            continue
        hr = s["hits"] / s["total"] * 100
        at = statistics.mean(s["tix"])
        mt = statistics.median(s["tix"])
        rr = s["ret"] / max(s["inv"], 1) * 100
        print(f"\nP{p} [{names[p]}]")
        print(f"  {s['total']}R 的中{s['hits']}({hr:.1f}%) 平均{at:.1f}点 中央値{mt:.0f}点 回収率{rr:.1f}%")
        for c in ["SS", "S", "A", "B", "C", "D", "E"]:
            bc = s["bc"].get(c)
            if not bc or bc["total"] == 0:
                continue
            bhr = bc["hits"] / bc["total"] * 100
            bat = statistics.mean(bc["tix"])
            brr = bc["ret"] / max(bc["inv"], 1) * 100
            print(f"  {c:>2}: {bc['total']:>4}R 的中{bc['hits']:>3}({bhr:>5.1f}%) 平均{bat:>5.1f}点 回収率{brr:>7.1f}%")

    # 現行フォーメーション
    print(f"\nP0 [現行フォーメーション（参考）]")
    tot = hits = inv0 = ret0 = 0
    tix_all = []
    bc0 = {}
    for pf in pred_files:
        ds = pf.stem.replace("_pred", "")
        rf = res_dir / (ds + "_results.json")
        if not rf.exists():
            continue
        pred = json.loads(pf.read_text(encoding="utf-8"))
        results = json.loads(rf.read_text(encoding="utf-8"))
        for race in pred.get("races", []):
            rid = race.get("race_id", "")
            if rid not in results:
                continue
            res = results[rid]
            order = res.get("order", [])
            if not order:
                continue
            top3 = set()
            for o in order:
                if isinstance(o, dict) and o.get("finish", 99) <= 3:
                    top3.add(o["horse_no"])
            if len(top3) < 3:
                continue
            ft = race.get("formation_tickets", [])
            san = [t for t in ft if t.get("type") == "三連複" and t.get("stake", 0) > 0]
            if not san:
                continue
            conf = race.get("confidence", "B")
            actual_payout = get_sanrenpuku_payout(res.get("payouts", {}))
            tot += 1
            n = len(san)
            tix_all.append(n)
            inv0 += n * 100
            if conf not in bc0:
                bc0[conf] = {"total": 0, "hits": 0, "tix": [], "inv": 0, "ret": 0}
            bc0[conf]["total"] += 1
            bc0[conf]["tix"].append(n)
            bc0[conf]["inv"] += n * 100
            for t in san:
                if set(t.get("combo", [])) == top3:
                    hits += 1
                    ret0 += actual_payout
                    bc0[conf]["hits"] += 1
                    bc0[conf]["ret"] += actual_payout
                    break
    if tot > 0:
        print(f"  {tot}R 的中{hits}({hits/tot*100:.1f}%) 平均{statistics.mean(tix_all):.1f}点 中央値{statistics.median(tix_all):.0f}点 回収率{ret0/max(inv0,1)*100:.1f}%")
        for c in ["SS", "S", "A", "B", "C", "D", "E"]:
            bc = bc0.get(c)
            if not bc or bc["total"] == 0:
                continue
            bhr = bc["hits"] / bc["total"] * 100
            bat = statistics.mean(bc["tix"])
            brr = bc["ret"] / max(bc["inv"], 1) * 100
            print(f"  {c:>2}: {bc['total']:>4}R 的中{bc['hits']:>3}({bhr:>5.1f}%) 平均{bat:>5.1f}点 回収率{brr:>7.1f}%")


if __name__ == "__main__":
    main()
