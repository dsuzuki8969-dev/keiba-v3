"""HTMLレポート用CSS定義"""

CSS = """
:root{--bg:#f2f4f8;--card:#fff;--border:#dde2ea;--navy:#0d2b5e;--gold:#c9952a;
  --red:#c0392b;--blue:#1a6fa8;--green:#1e8c4a;--warn:#d68910;--muted:#6b7280;--text:#1a1a2e;
  --bg2:#e8ecf2;--bg-alt:#edf0f7;--card-bg:#f8f9fc;--card-bg2:#f0f2f8}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Hiragino Sans","Yu Gothic UI",sans-serif;background:var(--bg);
  color:var(--text);font-size:14px;line-height:1.65}
.wrap{max-width:960px;margin:0 auto;padding:16px}
.race-title{background:linear-gradient(135deg,#0d2b5e 60%,#1a4a8a);color:#fff;
  border-radius:10px 10px 0 0;padding:20px 24px}
.race-title h1{font-size:22px;font-weight:700;margin-bottom:6px}
.race-title .race-info-line{font-size:13px;opacity:.9;margin:4px 0}
.race-title .sub{font-size:13px;opacity:.8}
.card{background:var(--card);border:1px solid var(--border);
  border-radius:0 0 8px 8px;padding:20px;margin-bottom:16px}
.card+.card{border-radius:8px}
.section-title{font-size:15px;font-weight:700;color:var(--navy);
  border-left:4px solid var(--gold);padding-left:10px;margin:20px 0 12px}
.sub-title{font-size:13px;font-weight:700;color:var(--navy);
  margin:14px 0 6px;border-bottom:1px solid var(--border);padding-bottom:4px}
/* 印: ◉/◎=緑, ○=青, ▲=赤, △=黄, ★=紫, ☆=青, ×=赤灰 */
.m-◉{color:#16a34a;font-weight:900;font-size:1.15em}
.m-◎{color:#16a34a;font-weight:900;font-size:1.1em}
.m-○{color:#2563eb;font-weight:700}
.m-▲{color:#dc2626;font-weight:700}
.m-△{color:#ca8a04;font-weight:700}
.m-★{color:#7c3aed}
.m-☆{color:#2563eb;font-weight:700}
.m-×{color:#dc2626;font-weight:700}
.m-－{color:#ccc}
/* バッジ */
.badge{display:inline-block;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:600}
.b-A{background:#1e8c4a;color:#fff}.b-B{background:var(--warn);color:#fff}
.b-C{background:var(--red);color:#fff}.b-SS{background:#16a34a;color:#fff}
.b-S{background:#1a6fa8;color:#fff}.b-A2{background:#c0392b;color:#fff}
.b-B2{background:#333;color:#fff}.b-C2{background:#333;color:#fff}
.b-D{background:#aaa;color:#fff}.b-E{background:#aaa;color:#fff}
.b-ana{background:var(--warn);color:#fff}.b-kiken{background:var(--red);color:#fff}
.b-tokusen{background:#2563eb;color:#fff;font-weight:700}
.b-tokusen-kiken{background:#dc2626;color:#fff;font-weight:700}
.b-pace{background:var(--blue);color:#fff}
/* テーブル */
table{width:100%;border-collapse:collapse;font-size:13px}
th{background:var(--navy);color:#fff;padding:8px 10px;font-weight:600;text-align:center}
td{padding:7px 10px;border-bottom:1px solid var(--border);text-align:right}
td:nth-child(1),td:nth-child(2){text-align:center}
td:nth-child(3){text-align:center}
td:nth-child(4),td:nth-child(5){text-align:left}
tr:hover td{background:#f0f4fb}
/* 枠番・馬番（netkeiba準拠枠色） */
.waku{display:inline-block;width:24px;height:24px;line-height:24px;text-align:center;
  border-radius:4px;font-weight:700;font-size:13px}
.uma{display:inline-block;width:24px;height:24px;line-height:24px;text-align:center;
  border-radius:50%;font-weight:700;font-size:13px}
/* 枠色 */
.wk1{background:#fff;color:#000;border:2px solid #999}
.wk2{background:#000;color:#fff}
.wk3{background:#e00;color:#fff}
.wk4{background:#0060e0;color:#fff}
.wk5{background:#ffd700;color:#000}
.wk6{background:#00a040;color:#fff}
.wk7{background:#f60;color:#fff}
.wk8{background:#f09;color:#fff}
/* 順位色は .rank-1/.rank-2/.rank-3 (緑/青/赤) に統一 — 下記165行目参照 */
/* 馬カード */
.hc{border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:14px}
.hc.top{border-left:5px solid var(--red)}
.hc.honmei{border-left:5px solid var(--navy)}
.hc.kiken{border-left:5px solid var(--red);background:#fff9f9}
.hc.oana{border-left:5px solid var(--warn)}
.hname{font-size:17px;font-weight:700}
.hmeta{font-size:12px;color:var(--muted);margin-top:2px}
.mlarge{font-size:30px;line-height:1;font-weight:900}
.devrow{display:flex;gap:12px;flex-wrap:wrap;margin:10px 0}
.di{text-align:center;background:#f0f4fb;border-radius:6px;padding:7px 14px;min-width:110px}
.di .dl{font-size:11px;color:var(--muted);display:block;margin-bottom:2px}
.di .dv{font-size:22px;font-weight:700;color:var(--navy);display:block}
.di .ds{font-size:11px;color:var(--muted)}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:10px}
.kv{display:flex;gap:6px;font-size:13px;margin-bottom:3px;flex-wrap:wrap}
.kv .k{color:var(--muted);min-width:110px;flex-shrink:0}
.cbox{background:#f8f9fb;border-radius:6px;padding:10px 14px;
  font-size:13px;line-height:1.8;margin-top:8px}
.plus::before{content:"✚ ";color:var(--green);font-weight:700}
.minus::before{content:"✖ ";color:var(--red);font-weight:700}
.plus{color:#1a4a2a;margin-bottom:2px}
.minus{color:#6e1010;margin-bottom:2px}
/* 断層 */
.danso{text-align:center;color:var(--gold);font-size:11px;font-weight:700;
  padding:3px 0;border-top:2px dashed var(--gold);margin:4px 0;letter-spacing:.05em}
/* 買い目 */
.ticket{display:flex;align-items:center;gap:10px;padding:10px 14px;
  background:#f4f7fb;border-radius:7px;margin-bottom:7px;flex-wrap:wrap}
.ticket.skip{background:#f9f9f9;opacity:0.6}
.ttype{font-weight:700;min-width:36px;color:var(--navy)}
.tcombo{font-size:18px;font-weight:700}
.tev{font-weight:700;font-size:14px;padding:2px 8px;border-radius:4px}
.ticket.strong .tev{background:#e8f4e8;color:var(--green)}
.ticket.normal .tev{background:#e8f0fa;color:var(--blue)}
.ticket.skip .tev{background:#f0f0f0;color:var(--muted)}
.tstake{margin-left:auto;font-weight:700;font-size:15px;color:var(--navy)}
.mishon{color:var(--muted);background:#f5f5f5;border-radius:6px;padding:12px 16px;font-size:13px}
/* 展開番号 */
.hno{background:var(--navy);color:#fff;border-radius:50%;width:27px;height:27px;
  display:inline-flex;align-items:center;justify-content:center;font-weight:700;font-size:12px}
.pg{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:6px}
.pg-label{font-size:12px;color:var(--muted);margin:4px 0 2px}
/* 前3走テーブル */
.past-table{width:100%;border-collapse:collapse;font-size:11px;margin-top:6px}
.past-table th{background:#eef1f8;color:var(--navy);font-weight:700;padding:4px 5px;
  text-align:center;white-space:nowrap;border-bottom:2px solid #ccd3e8}
.past-table td{padding:4px 5px;text-align:center;border-bottom:1px solid #eee;white-space:nowrap}
.past-table tr:hover td{background:#f5f7fc}
.past-dev-high{color:var(--green);font-weight:700}
.past-dev-mid{color:var(--blue);font-weight:700}
.past-dev-low{color:#c0392b;font-weight:700}
.past-pos-chip{display:inline-block;background:#e8ecf8;border-radius:3px;
  padding:1px 4px;font-size:10px;font-family:monospace}
.past-surface-芝{color:#1a7a3a;font-weight:700}
.past-surface-ダ{color:#8b5e2a;font-weight:700}
.past-grade{font-size:10px;color:var(--muted)}
.past-win{background:#fff8e6!important}
/* 上3F ランク色 (netkeiba 風) */
.l3f-r1{background:#d97706;color:#fff;font-weight:800;border-radius:4px;padding:2px 6px;letter-spacing:0.3px}
.l3f-r2{background:#1d4ed8;color:#fff;font-weight:800;border-radius:4px;padding:2px 6px;letter-spacing:0.3px}
.l3f-r3{background:#dc2626;color:#fff;font-weight:800;border-radius:4px;padding:2px 6px;letter-spacing:0.3px}
/* フォーメーション */
.form-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-bottom:16px}
.form-col{background:#f0f4fb;border-radius:8px;padding:10px 12px}
.form-col-title{font-size:11px;font-weight:700;color:var(--navy);margin-bottom:6px;letter-spacing:.05em}
.form-horse{display:inline-flex;align-items:center;gap:4px;margin:2px 3px;
  background:#fff;border-radius:4px;padding:2px 6px;font-size:13px;font-weight:700}
.form-horse .uma{border-radius:50%;
  width:22px;height:22px;display:inline-flex;align-items:center;justify-content:center;
  font-size:11px;margin-left:2px}
.ftkt{display:flex;align-items:center;gap:10px;padding:10px 14px;
  background:#f4f7fb;border-radius:8px;margin-bottom:6px;flex-wrap:wrap}
.ftkt.sig-hoshi{border-left:5px solid #c0392b;background:#fef7f7}
.ftkt.sig-maru2{border-left:5px solid #e67e22;background:#fef9f2}
.ftkt.sig-maru1{border-left:5px solid var(--green);background:#f3faf3}
.ftkt.sig-sankaku{border-left:4px solid var(--blue);background:#f4f7fb}
.ftkt.sig-batsu{border-left:3px solid #ccc;opacity:0.55}
.ftkt-type{font-weight:700;min-width:42px;font-size:13px;color:#fff;padding:2px 8px;border-radius:4px;text-align:center}
.ftkt-type-tansho{background:#16a34a}
.ftkt-type-umaren{background:#1a6fa8}
.ftkt-type-sanren{background:#c0392b}
.ftkt-combo{font-size:17px;font-weight:700}
.ftkt-signal{font-weight:900;font-size:18px;padding:4px 10px;border-radius:6px;min-width:36px;text-align:center}
.ftkt.sig-hoshi .ftkt-signal{background:#fde8e8;color:#c0392b}
.ftkt.sig-maru2 .ftkt-signal{background:#fdf0e0;color:#e67e22}
.ftkt.sig-maru1 .ftkt-signal{background:#e8f4e8;color:var(--green)}
.ftkt.sig-sankaku .ftkt-signal{background:#e8f0fa;color:var(--blue)}
.ftkt.sig-batsu .ftkt-signal{background:#f0f0f0;color:var(--muted)}
.ftkt-ev-detail{display:flex;align-items:baseline;gap:5px;flex-wrap:wrap}
.ftkt-ev-num{font-size:16px;font-weight:800}
.ftkt-ev-val{font-size:20px;font-weight:900;color:var(--navy)}
.ftkt-ev-label{font-size:11px;color:var(--muted)}
/* グレードバッジ */
/* 5色体系: SS=緑, S=青, A=赤, B/C=黒, D/E=灰 */
.grade-SS{color:#16a34a;font-weight:700}.grade-S{color:#2563eb;font-weight:700}
.grade-A{color:#dc2626;font-weight:700}.grade-B{color:var(--text);font-weight:700}
.grade-C{color:var(--text)}.grade-D{color:var(--muted)}.grade-E{color:var(--muted)}
/* 順位色: 1位=緑, 2位=青, 3位=赤 */
.rank-1{color:#16a34a;font-weight:700}.rank-2{color:#2563eb;font-weight:700}
.rank-3{color:#dc2626;font-weight:700}
.form-section-title{font-weight:700;font-size:13px;color:var(--navy);margin:12px 0 6px;
  border-bottom:1px solid #dde3ee;padding-bottom:4px}
@media(max-width:600px){.grid2{grid-template-columns:1fr}.devrow{gap:8px}.form-grid{grid-template-columns:1fr}
  .hds-grades{flex-wrap:wrap;gap:4px}.hds-comp{min-width:auto;font-size:12px}
  .hds-comment{min-width:auto}.ftkt{flex-wrap:wrap}
  .past-table{font-size:10px;min-width:680px}.past-table td,.past-table th{padding:3px 4px}
  .card{padding:10px 12px}}
/* モバイルでpast-tableは横スクロール許容（最低680pxで全カラム表示） */
/* 馬一覧アコーディオン */
.hd-list{margin-bottom:16px}
.hd-item{border:1px solid var(--border);border-radius:6px;margin-bottom:5px;overflow:hidden}
.hd-item[open]{border-color:#9ab3cc}
.hd-sum{display:flex;flex-direction:column;gap:2px;
  padding:8px 14px;cursor:pointer;list-style:none;background:#f8f9fb;font-size:12px}
.hd-sum::-webkit-details-marker{display:none}
.hd-sum::marker{display:none}
.hd-sum:hover{background:#eff3fb}
.hd-item[open]>.hd-sum{background:#eef1f9;border-bottom:1px solid var(--border)}
.hd-item.top>.hd-sum{border-left:4px solid var(--red)}
.hd-item.honmei>.hd-sum{border-left:4px solid var(--navy)}
.hd-item.oana>.hd-sum{border-left:4px solid var(--warn)}
.hd-item.kiken>.hd-sum{border-left:4px solid var(--red);background:#fff9f9}
.hd-body{padding:14px}
/* カード上段：枠・馬番・印・馬名・情報を1行に（モバイルでは折り返し） */
.hds-row1{display:flex;align-items:center;gap:3px 5px;flex-wrap:wrap}
.hds-row2{display:flex;align-items:center;gap:3px 8px;flex-wrap:wrap;padding-left:4px;margin-top:3px}
.hds-name{font-weight:700;font-size:13px;color:var(--navy)}
/* 三連率 */
.hds-wr-win{font-size:12px;font-weight:700}
.hds-wr-label{font-size:10px;color:var(--muted);margin-right:1px}
.hds-wr-sep{color:#d1d5db;margin:0 2px}
.hds-comment-inline{color:var(--muted);font-size:11px;margin-left:6px;font-style:italic;overflow:hidden;text-overflow:ellipsis}
.hds-comp{font-weight:800;min-width:52px;font-size:14px;text-align:right}
/* グレードバッジ */
.hds-grade-item{display:inline-flex;align-items:center;gap:2px;font-size:13px}
.hds-grade-label{font-size:11px;color:var(--muted);font-weight:600}
.hds-grades{display:flex;gap:8px;align-items:center}
.hds-comment{color:var(--muted);font-size:11px;flex:1;min-width:80px}
"""
