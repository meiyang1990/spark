#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Spark SQL 模块源码深度分析 PDF 生成器"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white, black, darkgrey
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak, Table, TableStyle
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
import os, platform

# ============ 字体 ============
def register_fonts():
    paths = {
        "Darwin": ["/System/Library/Fonts/STHeiti Medium.ttc",
                   "/System/Library/Fonts/Hiragino Sans GB.ttc",
                   "/Library/Fonts/Arial Unicode.ttf",
                   "/System/Library/Fonts/PingFang.ttc"],
        "Linux": ["/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"],
        "Windows": ["C:\\Windows\\Fonts\\msyh.ttc"],
    }
    for fp in paths.get(platform.system(), []):
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont("CF", fp))
                pdfmetrics.registerFont(TTFont("CFB", fp))
                return True
            except: continue
    return False

HCF = register_fonts()
FN = "CF" if HCF else "Helvetica"
FB = "CFB" if HCF else "Helvetica-Bold"

# ============ 颜色 ============
CP = HexColor("#1a237e"); CS = HexColor("#283593"); CA = HexColor("#0d47a1")
CTH = HexColor("#1565c0"); CTA = HexColor("#e3f2fd")
CO = HexColor("#e65100"); CG = HexColor("#2e7d32"); CPu = HexColor("#6a1b9a")

# ============ 样式 ============
def S():
    base = getSampleStyleSheet()
    return {
        'CT': ParagraphStyle('CT', parent=base['Title'], fontName=FB, fontSize=32, leading=42, textColor=CP, alignment=TA_CENTER, spaceAfter=10*mm),
        'CST': ParagraphStyle('CST', parent=base['Normal'], fontName=FN, fontSize=16, leading=24, textColor=CS, alignment=TA_CENTER, spaceAfter=5*mm),
        'H1': ParagraphStyle('H1', parent=base['Heading1'], fontName=FB, fontSize=22, leading=30, textColor=CP, spaceBefore=12*mm, spaceAfter=6*mm),
        'H2': ParagraphStyle('H2', parent=base['Heading2'], fontName=FB, fontSize=16, leading=22, textColor=CS, spaceBefore=8*mm, spaceAfter=4*mm),
        'H3': ParagraphStyle('H3', parent=base['Heading3'], fontName=FB, fontSize=13, leading=18, textColor=CA, spaceBefore=5*mm, spaceAfter=3*mm),
        'B': ParagraphStyle('B', parent=base['Normal'], fontName=FN, fontSize=10, leading=16, textColor=black, alignment=TA_JUSTIFY, spaceBefore=2*mm, spaceAfter=2*mm),
        'TH': ParagraphStyle('TH', parent=base['Normal'], fontName=FB, fontSize=9, leading=13, textColor=white, alignment=TA_CENTER),
        'TC': ParagraphStyle('TC', parent=base['Normal'], fontName=FN, fontSize=8.5, leading=12, textColor=black),
        'TB': ParagraphStyle('TB', parent=base['Normal'], fontName=FB, fontSize=8.5, leading=12, textColor=CA),
        'Cap': ParagraphStyle('Cap', parent=base['Normal'], fontName=FN, fontSize=9, leading=13, textColor=darkgrey, alignment=TA_CENTER, spaceBefore=2*mm, spaceAfter=4*mm),
        'BL': ParagraphStyle('BL', parent=base['Normal'], fontName=FN, fontSize=10, leading=15, textColor=black, leftIndent=10*mm, bulletIndent=5*mm, spaceBefore=1*mm, spaceAfter=1*mm),
    }

# ============ 工具函数 ============
def mt(headers, rows, cw=None):
    s = S()
    data = [[Paragraph(h, s['TH']) for h in headers]]
    for row in rows:
        data.append([Paragraph(str(c), s['TB'] if i==0 else s['TC']) for i,c in enumerate(row)])
    pw = A4[0] - 30*mm
    if not cw: cw = [pw/len(headers)] * len(headers)
    t = Table(data, colWidths=cw, repeatRows=1)
    st = [('BACKGROUND',(0,0),(-1,0),CTH), ('TEXTCOLOR',(0,0),(-1,0),white),
          ('ALIGN',(0,0),(-1,0),'CENTER'), ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
          ('GRID',(0,0),(-1,-1),0.5,HexColor("#bdbdbd")),
          ('TOPPADDING',(0,0),(-1,-1),4), ('BOTTOMPADDING',(0,0),(-1,-1),4),
          ('LEFTPADDING',(0,0),(-1,-1),5), ('RIGHTPADDING',(0,0),(-1,-1),5)]
    for i in range(1,len(data)):
        if i%2==0: st.append(('BACKGROUND',(0,i),(-1,i),CTA))
    t.setStyle(TableStyle(st))
    return t

def box(d,x,y,w,h,txt,fc,tc=white,fs=9):
    r = Rect(x,y,w,h,rx=4,ry=4); r.fillColor=fc; r.strokeColor=HexColor("#455a64"); r.strokeWidth=0.8; d.add(r)
    l = String(x+w/2, y+h/2-fs/3, txt); l.fontName=FB; l.fontSize=fs; l.fillColor=tc; l.textAnchor='middle'; d.add(l)

def arrow(d,x1,y1,x2,y2,c=HexColor("#455a64")):
    ln = Line(x1,y1,x2,y2); ln.strokeColor=c; ln.strokeWidth=1.2; d.add(ln)
    if y2<y1: d.add(Polygon(points=[x2,y2,x2-4,y2+6,x2+4,y2+6],fillColor=c,strokeColor=c,strokeWidth=0.5))
    elif x2>x1: d.add(Polygon(points=[x2,y2,x2-6,y2+4,x2-6,y2-4],fillColor=c,strokeColor=c,strokeWidth=0.5))

def lbl(d,x,y,txt,fs=7,c=darkgrey):
    l = String(x,y,txt); l.fontName="Helvetica"; l.fontSize=fs; l.fillColor=c; l.textAnchor='middle'; d.add(l)

# ============ 流程图1: 端到端流水线 ============
def fig_pipeline():
    d = Drawing(500,560)
    bg = Rect(0,0,500,560,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,540,"Spark SQL End-to-End Query Execution Pipeline"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    stages = [
        ("SQL / DataFrame API",          HexColor("#1565c0"), 500),
        ("Parser  (ANTLR4)",             HexColor("#0277bd"), 454),
        ("Analyzer  (60+ Rules)",        HexColor("#00838f"), 408),
        ("Optimizer  (50+ Rules)",       HexColor("#00695c"), 362),
        ("SparkPlanner (18 Strategies)", HexColor("#2e7d32"), 316),
        ("Preparation  (14 Rules)",      HexColor("#558b2f"), 270),
        ("WholeStageCodegen",            HexColor("#f9a825"), 224),
        ("AQE  (Adaptive Execution)",    HexColor("#e65100"), 178),
        ("Physical Execution (RDD)",     HexColor("#bf360c"), 132),
        ("Result  (collect/show/write)", HexColor("#4e342e"), 86),
    ]
    bw,bh,bx = 260,30,120
    for txt,color,y in stages: box(d,bx,y,bw,bh,txt,color,white,9)
    cx = bx+bw/2
    for i in range(len(stages)-1): arrow(d,cx,stages[i][2],cx,stages[i+1][2]+bh,HexColor("#546e7a"))

    notes = [("Unresolved LogicalPlan",454),("Resolved LogicalPlan",408),
             ("Optimized LogicalPlan",362),("SparkPlan",316),("ExecutedPlan",270)]
    for txt,y in notes:
        l = String(395,y+10,txt); l.fontName="Helvetica-Bold"; l.fontSize=7; l.fillColor=CPu; l.textAnchor='start'; d.add(l)
        ln = Line(bx+bw,y+bh/2,393,y+12); ln.strokeColor=HexColor("#ce93d8"); ln.strokeWidth=0.6; ln.strokeDashArray=[2,2]; d.add(ln)
    return d

# ============ 流程图2: 类继承体系 ============
def fig_hierarchy():
    d = Drawing(500,440)
    bg = Rect(0,0,500,440,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,422,"Catalyst Core Class Hierarchy"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    box(d,180,388,140,28,"TreeNode[T]",CP,white,10)
    box(d,50,335,140,28,"QueryPlan[T]",HexColor("#1565c0"),white,9)
    box(d,310,335,140,28,"Expression",HexColor("#00838f"),white,9)
    arrow(d,210,388,120,363,HexColor("#546e7a")); arrow(d,290,388,380,363,HexColor("#546e7a"))

    box(d,10,275,110,26,"LogicalPlan",HexColor("#0277bd"),white,8)
    box(d,130,275,100,26,"SparkPlan",CG,white,8)
    arrow(d,90,335,65,301,HexColor("#546e7a")); arrow(d,150,335,180,301,HexColor("#546e7a"))

    for i,(txt,y) in enumerate([("LeafNode",228),("UnaryNode",200),("BinaryNode",172)]):
        box(d,10,y,90,22,txt,HexColor("#0288d1"),white,7)
        arrow(d,55,275,55,y+22,HexColor("#90caf9"))
    for txt,y in [("Project",142),("Filter",120),("Join",98),("Aggregate",76)]:
        box(d,10,y,70,18,txt,HexColor("#e3f2fd"),HexColor("#0d47a1"),6.5)

    for txt,y in [("LeafExecNode",228),("UnaryExecNode",200),("BinaryExecNode",172)]:
        box(d,130,y,100,22,txt,HexColor("#388e3c"),white,7)
        arrow(d,180,275,180,y+22,HexColor("#a5d6a7"))
    for txt,y in [("ProjectExec",142),("FilterExec",120),("SortMergeJoinExec",98),("HashAggExec",76)]:
        box(d,130,y,100,18,txt,HexColor("#e8f5e9"),HexColor("#1b5e20"),6)

    for txt,x,y in [("LeafExpr",280,288),("UnaryExpr",280,260),("BinaryExpr",280,232),
                     ("NamedExpr",400,288),("AggFunc",400,260),("SubqueryExpr",400,232)]:
        box(d,x,y,105,22,txt,HexColor("#00695c"),white,7)
        arrow(d,380,335,x+52,y+22,HexColor("#80cbc4"))
    for txt,x,y in [("Literal",280,198),("Cast",280,176),("Add/Sub/Mul",280,154),
                     ("AttributeRef",400,198),("Alias",400,176),("Count/Sum",400,154)]:
        box(d,x,y,105,18,txt,HexColor("#e0f2f1"),HexColor("#004d40"),6)

    box(d,300,100,180,26,"RuleExecutor[TreeType]",CPu,white,8)
    box(d,300,60,85,22,"Analyzer",HexColor("#7b1fa2"),white,7)
    box(d,395,60,85,22,"Optimizer",HexColor("#7b1fa2"),white,7)
    arrow(d,360,100,342,82,HexColor("#ce93d8")); arrow(d,420,100,437,82,HexColor("#ce93d8"))
    box(d,300,28,180,20,"Rule[T]: apply(plan)=>plan",HexColor("#e1bee7"),HexColor("#4a148c"),7)
    arrow(d,390,60,390,48,HexColor("#ce93d8"))
    return d

# ============ 流程图3: QueryExecution 阶段 ============
def fig_qe():
    d = Drawing(500,520)
    bg = Rect(0,0,500,520,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,500,"QueryExecution Detailed Phases"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    phases = [("1. logical",HexColor("#1565c0"),465),("2. analyzed",HexColor("#0277bd"),420),
              ("3. commandExecuted",HexColor("#00838f"),375),("4. withCachedData",HexColor("#00695c"),330),
              ("5. optimizedPlan",HexColor("#2e7d32"),285),("6. sparkPlan",HexColor("#558b2f"),240),
              ("7. executedPlan",HexColor("#f57f17"),195),("8. toRdd",HexColor("#bf360c"),150)]
    bw,bh,bx = 180,30,20
    for txt,color,y in phases: box(d,bx,y,bw,bh,txt,color,white,8.5)
    cx = bx+bw/2
    for i in range(len(phases)-1): arrow(d,cx,phases[i][2],cx,phases[i+1][2]+bh,HexColor("#546e7a"))

    rules = ["InsertAdaptiveSparkPlan","EnsureRequirements","PlanDynamicPruning",
             "PlanSubqueries","RemoveRedundantProjects","CollapseCodegenStages",
             "ReuseExchangeAndSubquery","ReplaceHashWithSortAgg","RemoveRedundantSorts"]
    px = 260
    lt = String(370,215,"Preparation Rules (Phase 7)"); lt.fontName="Helvetica-Bold"; lt.fontSize=8; lt.fillColor=CO; lt.textAnchor='middle'; d.add(lt)
    for i,r in enumerate(rules):
        box(d,px,200-i*17,210,14,r,HexColor("#fff3e0"),CO,5.5)
    ln = Line(bx+bw,210,px,205); ln.strokeColor=CO; ln.strokeWidth=0.8; ln.strokeDashArray=[3,2]; d.add(ln)
    return d

# ============ 流程图4: AQE ============
def fig_aqe():
    d = Drawing(500,400)
    bg = Rect(0,0,500,400,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,382,"Adaptive Query Execution (AQE) Flow"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    steps = [("Initial SparkPlan",HexColor("#1565c0"),345),("Create QueryStages",HexColor("#0277bd"),298),
             ("Execute Stages (async)",HexColor("#00838f"),251),("Collect Runtime Stats",HexColor("#00695c"),204),
             ("Re-optimize (AQEOptimizer)",CO,157),("Compare Cost",HexColor("#bf360c"),110),
             ("Final Plan",CG,63)]
    bw,bh,bx = 220,30,140
    for txt,color,y in steps: box(d,bx,y,bw,bh,txt,color,white,9)
    cx = bx+bw/2
    for i in range(len(steps)-1): arrow(d,cx,steps[i][2],cx,steps[i+1][2]+bh,HexColor("#546e7a"))

    xr = bx+bw+15
    for (x1,y1,x2,y2) in [(xr,125,xr,313),(xr,313,bx+bw,313),(bx+bw,125,xr,125)]:
        ln = Line(x1,y1,x2,y2); ln.strokeColor=CO; ln.strokeWidth=1.2; ln.strokeDashArray=[4,2]; d.add(ln)
    l = String(xr+8,220,"Loop until all"); l.fontName="Helvetica-Bold"; l.fontSize=7; l.fillColor=CO; l.textAnchor='start'; d.add(l)
    l2 = String(xr+8,210,"stages done"); l2.fontName="Helvetica"; l2.fontSize=7; l2.fillColor=CO; l2.textAnchor='start'; d.add(l2)

    for i,r in enumerate(["CoalesceShufflePartitions","OptimizeSkewedJoin","OptimizeShuffleLocalRead","DynamicPruningFilters"]):
        box(d,5,235-i*18,130,15,r,HexColor("#fff8e1"),CO,5.5)
    return d

# ============ 流程图5: Join策略 ============
def fig_join():
    d = Drawing(500,380)
    bg = Rect(0,0,500,380,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,362,"JoinSelection Strategy Decision Flow"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    box(d,175,328,150,26,"Join LogicalPlan",CP,white,9)
    box(d,185,282,130,26,"Has Join Hint?",HexColor("#f57f17"),white,8)
    arrow(d,250,328,250,308,HexColor("#546e7a"))
    box(d,350,282,130,26,"Follow Hint",CO,white,8)
    arrow(d,315,295,350,295,HexColor("#546e7a"))

    box(d,185,236,130,26,"Equi-Join?",HexColor("#f57f17"),white,8)
    arrow(d,250,282,250,262,HexColor("#546e7a"))

    for i,(txt,c,y) in enumerate([("BroadcastHashJoin",HexColor("#1565c0"),190),
                                   ("ShuffledHashJoin",HexColor("#0277bd"),158),
                                   ("SortMergeJoin",HexColor("#00838f"),126)]):
        box(d,20,y,150,26,txt,c,white,8)
        if i>0: arrow(d,95,y+26+6,95,y+26,HexColor("#90caf9"))
    arrow(d,185,249,170,216,HexColor("#546e7a"))

    for i,(txt,c,y) in enumerate([("BroadcastNestedLoopJoin",CPu,190),("CartesianProduct",HexColor("#4a148c"),158)]):
        box(d,330,y,160,26,txt,c,white,8)
        if i>0: arrow(d,410,y+26+6,410,y+26,HexColor("#ce93d8"))
    arrow(d,315,249,410,216,HexColor("#546e7a"))

    box(d,50,60,400,28,"Decision: table size, broadcast threshold, join type, hints",HexColor("#e8eaf6"),CP,7.5)
    return d

# ============ 流程图6: Codegen ============
def fig_codegen():
    d = Drawing(500,340)
    bg = Rect(0,0,500,340,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,322,"WholeStageCodegen: Produce-Consume Model"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    outer = Rect(30,45,200,255,rx=4,ry=4); outer.fillColor=HexColor("#e8eaf6"); outer.strokeColor=CP; outer.strokeWidth=1.5; d.add(outer)
    l = String(130,288,"WholeStageCodegenExec"); l.fontName=FB; l.fontSize=8; l.fillColor=CP; l.textAnchor='middle'; d.add(l)

    ops = [("ProjectExec",HexColor("#1565c0"),250),("FilterExec",HexColor("#0277bd"),205),("ScanExec",HexColor("#00838f"),160)]
    for txt,c,y in ops: box(d,55,y,150,30,txt,c,white,9)
    for i in range(len(ops)-1):
        y1,y2 = ops[i][2],ops[i+1][2]+30
        ln = Line(50,y1+15,50,y2+15); ln.strokeColor=CG; ln.strokeWidth=1.5; d.add(ln)
        l = String(38,(y1+y2+30)/2+3,"produce()"); l.fontName="Helvetica"; l.fontSize=6; l.fillColor=CG; l.textAnchor='end'; d.add(l)

    for i in range(len(ops)-1,0,-1):
        y1,y2 = ops[i][2],ops[i-1][2]+30
        ln = Line(210,y1+15,210,y2-10); ln.strokeColor=CO; ln.strokeWidth=1.5; d.add(ln)
        d.add(Polygon(points=[210,y2-10,207,y2-4,213,y2-4],fillColor=CO,strokeColor=CO,strokeWidth=0.5))
        l = String(220,(y1+y2+15)/2,"consume()"); l.fontName="Helvetica"; l.fontSize=6; l.fillColor=CO; l.textAnchor='start'; d.add(l)

    box(d,55,100,150,26,"InputAdapter",HexColor("#f57f17"),white,8)
    arrow(d,130,160,130,126,HexColor("#546e7a"))
    box(d,55,55,150,26,"External RDD",HexColor("#795548"),white,8)
    arrow(d,130,100,130,81,HexColor("#546e7a"))

    cx = 260
    box(d,cx,255,210,28,"Generated Java Code",CG,white,9)
    for i,ln in enumerate(["class GeneratedIterator","  extends BufferedRowIterator {","  void processNext() {",
                            "    // Scan -> Filter -> Project","    // fused into single method","    append(outputRow);","  }","}"]):
        l = String(cx+10,238-i*13,ln); l.fontName="Courier"; l.fontSize=7; l.fillColor=HexColor("#212121"); d.add(l)

    box(d,cx,80,210,24,"Performance Benefits",CO,white,8)
    for i,b in enumerate(["No virtual function calls","CPU register-friendly","Loop unrolling + SIMD"]):
        l = String(cx+10,66-i*13,"* "+b); l.fontName="Helvetica"; l.fontSize=7; l.fillColor=HexColor("#616161"); d.add(l)
    return d

# ============ 流程图7: 模块总览 ============
def fig_modules():
    d = Drawing(500,400)
    bg = Rect(0,0,500,400,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,382,"Spark SQL Module Architecture Overview"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    layers = [
        ("sql/api (Public Abstract API: SparkSession, Dataset, Column)",30,348,440,26,HexColor("#1565c0")),
        ("sql/connect (gRPC Client-Server)",30,308,210,26,CPu),
        ("sql/core (Classic Execution Engine)",250,308,220,26,CG),
        ("sql/catalyst (Query Optimization Framework)",30,264,440,32,HexColor("#00838f")),
        ("sql/hive (Hive Metastore)",30,228,210,26,CO),
        ("sql/pipelines (Declarative Dataflow)",250,228,220,26,HexColor("#795548")),
        ("sql/hive-thriftserver (JDBC/ODBC)",30,192,440,26,HexColor("#455a64")),
    ]
    for txt,x,y,w,h,c in layers: box(d,x,y,w,h,txt,c,white,7.5)

    # Catalyst 内部
    oy = 130
    outer = Rect(25,oy-10,450,50,rx=3,ry=3); outer.fillColor=HexColor("#e0f2f1"); outer.strokeColor=HexColor("#00838f"); outer.strokeWidth=1; d.add(outer)
    l = String(250,oy+32,"Catalyst Internals"); l.fontName=FB; l.fontSize=8; l.fillColor=HexColor("#004d40"); l.textAnchor='middle'; d.add(l)
    for txt,x,w,c in [("Parser",30,80,HexColor("#0277bd")),("Analyzer",120,80,HexColor("#00838f")),
                       ("Optimizer",210,80,HexColor("#00695c")),("TreeNode",300,80,CG),("Rules",390,80,HexColor("#558b2f"))]:
        box(d,x,oy,w,22,txt,c,white,8)

    # Core 内部
    cy = 55
    outer2 = Rect(25,cy-10,450,50,rx=3,ry=3); outer2.fillColor=HexColor("#e8f5e9"); outer2.strokeColor=CG; outer2.strokeWidth=1; d.add(outer2)
    l2 = String(250,cy+32,"Execution Engine (sql/core)"); l2.fontName=FB; l2.fontSize=8; l2.fillColor=HexColor("#1b5e20"); l2.textAnchor='middle'; d.add(l2)
    for txt,x,w,c in [("Planner",30,85,HexColor("#388e3c")),("SparkPlan",125,80,HexColor("#43a047")),
                       ("Codegen",215,75,HexColor("#558b2f")),("AQE",300,60,CO),("ExecOps",370,100,HexColor("#bf360c"))]:
        box(d,x,cy,w,22,txt,c,white,7)
    return d

# ============ 流程图8: Spark Connect 架构 ============
def fig_connect():
    d = Drawing(500,330)
    bg = Rect(0,0,500,330,rx=6,ry=6); bg.fillColor=HexColor("#fafafa"); bg.strokeColor=HexColor("#e0e0e0"); bg.strokeWidth=0.5; d.add(bg)
    t = String(250,312,"Spark Connect Architecture"); t.fontName=FB; t.fontSize=12; t.fillColor=CP; t.textAnchor='middle'; d.add(t)

    box(d,20,255,140,32,"Client (Py/Scala/Java)",HexColor("#1565c0"),white,9)
    box(d,20,215,140,28,"Proto Plan (Protobuf)",HexColor("#e3f2fd"),CP,8)
    box(d,185,235,80,28,"gRPC",HexColor("#f57f17"),white,10)
    arrow(d,160,248,185,248,HexColor("#546e7a")); arrow(d,265,248,290,248,HexColor("#546e7a"))

    sx = 290
    box(d,sx,255,190,32,"SparkConnectService",CG,white,9)
    box(d,sx,215,190,28,"SparkConnectPlanner",HexColor("#388e3c"),white,8)
    box(d,sx,175,190,28,"SparkSession + QueryExec",HexColor("#43a047"),white,8)
    arrow(d,sx+95,255,sx+95,243,HexColor("#546e7a")); arrow(d,sx+95,215,sx+95,203,HexColor("#546e7a"))
    box(d,sx,120,190,32,"Spark Cluster (Executors)",HexColor("#bf360c"),white,9)
    arrow(d,sx+95,175,sx+95,152,HexColor("#546e7a"))

    box(d,20,150,140,24,"SessionManager",CPu,white,8)
    box(d,20,118,140,24,"ExecutionManager",HexColor("#7b1fa2"),white,8)
    for i,f in enumerate(["Language-agnostic client","Thin client (no Spark deps)","Reconnectable sessions","Arrow format results"]):
        l = String(20,90-i*13,"* "+f); l.fontName="Helvetica"; l.fontSize=7; l.fillColor=HexColor("#616161"); d.add(l)
    return d

# ============ 构建 PDF ============
def build_pdf():
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Spark_SQL_源码深度分析.pdf")
    doc = SimpleDocTemplate(out, pagesize=A4, topMargin=20*mm, bottomMargin=20*mm, leftMargin=15*mm, rightMargin=15*mm)
    s = S(); E = []; pw = A4[0]-30*mm

    # ---- 封面 ----
    E.append(Spacer(1,50*mm))
    E.append(Paragraph("Spark SQL", s['CT']))
    E.append(Paragraph("源码深度分析", s['CT']))
    E.append(Spacer(1,10*mm))
    E.append(Paragraph("核心流程图 · 类设计说明 · 架构解析", s['CST']))
    E.append(Paragraph("基于 Apache Spark v3.3.1 源码", s['CST']))
    E.append(Spacer(1,8*mm))
    E.append(Paragraph("涵盖: catalyst · core · hive · connect · api · pipelines", s['CST']))
    E.append(PageBreak())

    # ---- 目录 ----
    E.append(Paragraph("目 录", s['H1']))
    toc = [("第一章","Spark SQL 模块总览架构"),("第二章","端到端查询执行流程"),("第三章","Catalyst 优化框架核心设计"),
           ("第四章","QueryExecution 执行阶段详解"),("第五章","物理计划生成与 Join 策略选择"),("第六章","WholeStageCodegen 全阶段代码生成"),
           ("第七章","AQE 自适应查询执行"),("第八章","Spark Connect 远程架构"),("第九章","Hive 集成与 Pipelines 模块"),
           ("第十章","核心类设计总结"),("第十一章","总结与架构评价")]
    toc_data = [[Paragraph(n,s['TB']),Paragraph(t,s['TC'])] for n,t in toc]
    tt = Table(toc_data, colWidths=[pw*0.2,pw*0.8])
    tt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'MIDDLE'),('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),('LINEBELOW',(0,0),(-1,-1),0.5,HexColor("#e0e0e0"))]))
    E.append(tt); E.append(PageBreak())

    # ======== 第一章 ========
    E.append(Paragraph("第一章 Spark SQL 模块总览架构", s['H1']))
    E.append(Paragraph("Spark SQL 是 Apache Spark 最核心的模块之一，提供了从 SQL 解析到分布式执行的完整查询处理能力。整个 sql/ 目录包含 7 个子模块，各司其职又紧密协作，形成了层次分明、高度可扩展的查询引擎架构。", s['B']))
    E.append(Paragraph("1.1 模块架构总览图", s['H2']))
    E.append(fig_modules())
    E.append(Paragraph("图 1-1: Spark SQL 模块架构总览（层次依赖关系）", s['Cap']))

    E.append(Paragraph("1.2 各模块功能定位", s['H2']))
    E.append(mt(["模块","文件数","功能定位","关键特征"],
        [["sql/api","26","公共抽象 API 层，定义 SparkSession/Dataset/Column 等接口","抽象类，Classic/Connect 双实现"],
         ["sql/catalyst","350+","查询优化框架：SQL 解析 → 语义分析 → 逻辑优化","TreeNode 统一树结构，规则化转换"],
         ["sql/core","200+","经典执行引擎：物理计划生成 → 代码生成 → 分布式执行","SparkPlanner 策略模式，Codegen"],
         ["sql/hive","33","Hive Metastore 集成，Hive 表/UDF/SerDe 支持","类加载隔离，多版本兼容"],
         ["sql/connect","67","远程客户端-服务端架构，gRPC 协议通信","Protobuf 序列化，语言无关"],
         ["sql/pipelines","64","声明式数据流管道（类似 Delta Live Tables）","数据流图，拓扑排序执行"]],
        [pw*0.12,pw*0.07,pw*0.46,pw*0.35]))
    E.append(PageBreak())

    # ======== 第二章 ========
    E.append(Paragraph("第二章 端到端查询执行流程", s['H1']))
    E.append(Paragraph("当用户调用 spark.sql(\"SELECT ...\") 或通过 DataFrame API 构建查询时，Spark SQL 会经历一系列精心设计的处理阶段。这个流水线是理解整个 Spark SQL 架构的核心。", s['B']))
    E.append(fig_pipeline())
    E.append(Paragraph("图 2-1: Spark SQL 端到端查询执行流水线（10 个阶段）", s['Cap']))

    E.append(Paragraph("2.1 各阶段详解", s['H2']))
    E.append(mt(["阶段","职责","核心说明"],
        [["Parser","SQL 解析","ANTLR4 词法/语法分析 → AstBuilder → Unresolved LogicalPlan"],
         ["Analyzer","语义分析","60+ 条规则 FixedPoint 迭代：解析表/列/函数/类型"],
         ["Optimizer","逻辑优化","50+ 条规则：谓词下推、列裁剪、常量折叠、Join 重排"],
         ["SparkPlanner","物理计划","18 个 Strategy 将 LogicalPlan → SparkPlan"],
         ["Preparation","执行准备","14 条 rules：InsertAQE、EnsureRequirements、Codegen"],
         ["Codegen","代码生成","融合算子为单 Java 方法，消除虚函数调用"],
         ["AQE","自适应执行","运行时统计 → 动态调整分区/Join 策略/倾斜优化"],
         ["Execution","物理执行","通过 Spark Core RDD 引擎分布式执行"]],
        [pw*0.13,pw*0.1,pw*0.77]))
    E.append(PageBreak())

    # ======== 第三章 ========
    E.append(Paragraph("第三章 Catalyst 优化框架核心设计", s['H1']))
    E.append(Paragraph("Catalyst 的设计精髓在于三个核心抽象：统一的树结构（TreeNode）、规则化的转换引擎（RuleExecutor）和不可变的函数式转换。", s['B']))
    E.append(Paragraph("3.1 核心类继承体系", s['H2']))
    E.append(fig_hierarchy())
    E.append(Paragraph("图 3-1: Catalyst 核心类继承体系", s['Cap']))

    E.append(Paragraph("3.2 TreeNode —— 万物之基", s['H2']))
    E.append(Paragraph("TreeNode 是 Catalyst 整个树形结构的根基类。所有逻辑计划、物理计划和表达式都继承自它，提供不可变的树结构操作 API。", s['B']))
    E.append(mt(["方法","返回类型","说明"],
        [["transform(rule)","TreeNode","递归应用转换规则（默认 top-down）"],
         ["transformDown/Up","TreeNode","前序/后序递归转换"],
         ["transformWithPruning","TreeNode","带 BitSet 剪枝的转换（性能关键）"],
         ["foreach/foreachUp","Unit","前序/后序遍历"],
         ["collect/collectFirst","Seq/Option","收集满足条件的节点"],
         ["withNewChildren","TreeNode","创建具有新子节点的副本"],
         ["fastEquals","Boolean","快速相等判断（先引用比较再值比较）"]],
        [pw*0.28,pw*0.12,pw*0.60]))

    E.append(Paragraph("3.3 RuleExecutor —— 规则引擎", s['H2']))
    E.append(mt(["概念","说明","示例"],
        [["Rule[T]","单条转换规则：apply(plan) => plan","ConstantFolding, PushDownPredicates"],
         ["Batch","一组规则 + 执行策略","Batch(\"Opt\", FixedPoint(100), rules...)"],
         ["Once","规则只执行一次","Finish Analysis"],
         ["FixedPoint(n)","迭代直到不变或达 n 次","Resolution 批次"]],
        [pw*0.15,pw*0.45,pw*0.40]))

    E.append(Paragraph("3.4 性能优化机制", s['H2']))
    for t,d in [("TreePattern BitSet 剪枝","每个节点维护 BitSet 记录子树包含的模式类型，transform 时跳过不匹配的子树"),
                ("RuleId 无效规则剪枝","记录已证明无效的规则 ID，后续迭代跳过该规则"),
                ("不可变树 + 结构共享","只复制变化路径上的节点，未变化子树直接复用引用"),
                ("fastEquals 快速比较","先引用比较，大多数情况避免深度值比较")]:
        E.append(Paragraph(f"<b>• {t}</b>：{d}", s['B']))
    E.append(PageBreak())

    # ======== 第四章 ========
    E.append(Paragraph("第四章 QueryExecution 执行阶段详解", s['H1']))
    E.append(Paragraph("QueryExecution 是查询生命周期的核心管理者，通过 LazyTry 惰性求值逐步将逻辑计划转化为可执行的物理计划。", s['B']))
    E.append(fig_qe())
    E.append(Paragraph("图 4-1: QueryExecution 8 个执行阶段与 Preparation Rules", s['Cap']))

    E.append(Paragraph("4.1 Preparation Rules 详解", s['H2']))
    E.append(mt(["规则","说明"],
        [["InsertAdaptiveSparkPlan","在计划根部插入 AQE 节点"],
         ["EnsureRequirements","插入 Shuffle/Sort 满足数据分布和排序需求"],
         ["PlanDynamicPruningFilters","运行时动态分区裁剪"],
         ["PlanSubqueries","为子查询生成独立物理计划"],
         ["CollapseCodegenStages","折叠连续 codegen 算子为 WholeStageCodegenExec"],
         ["ReuseExchangeAndSubquery","复用相同的 Exchange 和子查询结果"],
         ["ReplaceHashWithSortAgg","排序数据上将 HashAgg 替换为 SortAgg"],
         ["RemoveRedundantSorts","移除已满足排序需求的冗余 Sort"]],
        [pw*0.35,pw*0.65]))
    E.append(PageBreak())

    # ======== 第五章 ========
    E.append(Paragraph("第五章 物理计划生成与 Join 策略选择", s['H1']))
    E.append(Paragraph("5.1 SparkPlanner 策略列表", s['H2']))
    E.append(Paragraph("SparkPlanner 通过策略模式将逻辑计划节点转换为物理计划节点。18 个策略按优先级依次尝试匹配。", s['B']))
    E.append(mt(["优先级","策略","说明"],
        [["1","extraStrategies","用户自定义策略"],
         ["2-3","LogicalQueryStageStrategy","AQE 已物化 Stage"],
         ["4","PythonEvals","Python UDF 评估"],
         ["5-7","DataSourceV2/File/DataSource","数据源策略"],
         ["8","SpecialLimits","Limit 优化（TakeOrderedAndProject）"],
         ["9","Aggregation","Hash/Sort/Object 聚合"],
         ["10-11","Window / WindowGroupLimit","窗口函数"],
         ["12","JoinSelection","Join 策略选择（最复杂）"],
         ["13","InMemoryScans","缓存表扫描"],
         ["14","BasicOperators","兜底：Project/Filter/Sort/Union 等"]],
        [pw*0.1,pw*0.28,pw*0.62]))

    E.append(Paragraph("5.2 Join 策略决策流程", s['H2']))
    E.append(fig_join())
    E.append(Paragraph("图 5-1: Join 策略选择决策流程", s['Cap']))

    E.append(Paragraph("5.3 Join 物理算子对比", s['H2']))
    E.append(mt(["算子","原理","适用条件","性能"],
        [["BroadcastHashJoin","广播小表到所有 Executor，本地 Hash","小表<10MB","O(n), 最快"],
         ["ShuffledHashJoin","Shuffle 后分区内 Hash Join","一侧明显较小","较快"],
         ["SortMergeJoin","Shuffle+排序后归并连接","两侧都很大","O(n log n), 稳定"],
         ["BroadcastNLJ","广播+嵌套循环","非等值Join+小表","O(n*m)"],
         ["CartesianProduct","笛卡尔积","无Join条件","O(n*m), 最慢"]],
        [pw*0.2,pw*0.32,pw*0.25,pw*0.23]))
    E.append(PageBreak())

    # ======== 第六章 ========
    E.append(Paragraph("第六章 WholeStageCodegen 全阶段代码生成", s['H1']))
    E.append(Paragraph("WholeStageCodegen 将连续支持 codegen 的算子融合为单个 Java 方法，消除火山模型的虚函数调用开销，使 CPU 流水线利用率最大化。", s['B']))
    E.append(fig_codegen())
    E.append(Paragraph("图 6-1: Produce-Consume 模型与代码生成", s['Cap']))

    E.append(Paragraph("6.1 CodegenSupport 接口", s['H2']))
    E.append(mt(["方法","返回","说明"],
        [["produce(ctx,parent)","String","生产者：生成数据产生代码"],
         ["consume(ctx,vars,row)","String","消费者：向父节点传递数据"],
         ["doProduce(ctx)","String","子类实现的生产逻辑"],
         ["doConsume(ctx,input,row)","String","子类实现的消费逻辑"],
         ["inputRDDs()","Seq[RDD]","叶子节点的输入 RDD"],
         ["supportCodegen","Boolean","是否支持代码生成"]],
        [pw*0.27,pw*0.1,pw*0.63]))
    E.append(PageBreak())

    # ======== 第七章 ========
    E.append(Paragraph("第七章 AQE 自适应查询执行", s['H1']))
    E.append(Paragraph("AQE 在运行时根据实际数据统计动态调整查询计划。核心思想：将执行分为多个 Stage，每个 Stage 完成后收集统计，重新优化后续 Stage。", s['B']))
    E.append(fig_aqe())
    E.append(Paragraph("图 7-1: AQE 执行循环流程", s['Cap']))

    E.append(Paragraph("7.1 AQE 核心优化规则", s['H2']))
    E.append(mt(["规则","功能","原理"],
        [["CoalesceShufflePartitions","合并过小 Shuffle 分区","基于 advisoryPartitionSizeInBytes"],
         ["OptimizeSkewedJoin","倾斜 Join 优化","拆分倾斜分区为多个子任务"],
         ["OptimizeShuffleWithLocalRead","本地读取替代 Shuffle","同 Executor 直接读取"],
         ["DynamicPruningFilters","运行时分区裁剪","基于 Broadcast 结果过滤"]],
        [pw*0.3,pw*0.25,pw*0.45]))
    E.append(PageBreak())

    # ======== 第八章 ========
    E.append(Paragraph("第八章 Spark Connect 远程架构", s['H1']))
    E.append(Paragraph("Spark Connect 实现客户端-服务端解耦，轻量级客户端通过 gRPC 远程提交查询。客户端无需 Spark 依赖，支持 Python/Scala/Java 等多语言。", s['B']))
    E.append(fig_connect())
    E.append(Paragraph("图 8-1: Spark Connect 架构", s['Cap']))

    E.append(Paragraph("8.1 核心组件", s['H2']))
    E.append(mt(["组件","职责","说明"],
        [["SparkConnectService","gRPC 服务入口","处理 executePlan/analyzePlan/config 等 RPC"],
         ["SparkConnectPlanner","计划转换器(170KB)","Proto Relation → Catalyst LogicalPlan"],
         ["SessionHolder","会话状态","管理 SparkSession、DataFrame 缓存等"],
         ["ExecuteHolder","执行状态","请求/job tag/可重连/结果分块"],
         ["SessionManager","全局会话管理","创建/获取/关闭/定时清理会话"],
         ["ExecutionManager","全局执行管理","跟踪所有执行状态，墓碑缓存"]],
        [pw*0.22,pw*0.2,pw*0.58]))
    E.append(PageBreak())

    # ======== 第九章 ========
    E.append(Paragraph("第九章 Hive 集成与 Pipelines 模块", s['H1']))
    E.append(Paragraph("9.1 Hive 集成模块", s['H2']))
    E.append(Paragraph("sql/hive 模块负责与 Hive Metastore 的集成，通过类加载隔离支持多版本 Hive 兼容。", s['B']))
    E.append(mt(["核心类","说明"],
        [["HiveExternalCatalog","ExternalCatalog 的 Hive 实现，所有元数据操作委托给 HiveClient"],
         ["HiveClient / HiveClientImpl","与 Hive Metastore 交互的接口和实现（反射调用 Hive API）"],
         ["IsolatedClientLoader","用独立 URLClassLoader 加载指定版本 Hive jar，实现类加载隔离"],
         ["HiveSessionStateBuilder","构建 Hive 感知的 SessionState，注入 Hive UDF 和优化规则"],
         ["HiveTableScanExec","Hive 表扫描物理算子，支持列裁剪和分区裁剪"],
         ["TableReader","读取 Hive 表数据产出 RDD[InternalRow]"]],
        [pw*0.3,pw*0.7]))

    E.append(Paragraph("Hive Metastore 交互链路", s['H3']))
    E.append(Paragraph("SparkSession → HiveSessionStateBuilder → HiveSessionCatalog → HiveExternalCatalog → HiveUtils.newClientForMetadata() → IsolatedClientLoader → HiveClientImpl → HiveMetaStoreClient → Thrift RPC → Hive Metastore 服务", s['B']))

    E.append(Paragraph("9.2 Pipelines 模块", s['H2']))
    E.append(Paragraph("sql/pipelines 实现声明式数据流管道框架，用户声明数据流图，系统自动处理依赖解析和拓扑排序执行。", s['B']))
    E.append(mt(["核心类","说明"],
        [["DataflowGraph","核心图结构，管理 Flow/Table/Sink/View 及其关系"],
         ["Flow + FlowFunction","数据流节点，FlowFunction 是用户查询 lambda"],
         ["PipelineExecution","执行入口：resolveGraph → materialize → execute"],
         ["TriggeredGraphExecution","按拓扑顺序执行所有 Flow，支持失败重试"],
         ["DatasetManager","在 Catalog 中物化表，处理 Schema 合并"],
         ["SqlGraphRegistrationContext","解析 SQL DDL 注册图节点"]],
        [pw*0.3,pw*0.7]))
    E.append(PageBreak())

    # ======== 第十章 ========
    E.append(Paragraph("第十章 核心类设计总结", s['H1']))

    E.append(Paragraph("10.1 SparkSession —— 程序入口", s['H2']))
    E.append(Paragraph("SparkSession 是 Spark SQL 的编程入口，内部持有 SharedState（跨 Session 共享的 CacheManager/ExternalCatalog）和 SessionState（会话级的 Analyzer/Optimizer/Planner/SQLConf）。", s['B']))
    E.append(mt(["成员","说明"],
        [["sharedState","共享状态：CacheManager, ExternalCatalog, StatusTracker"],
         ["sessionState","会话状态：Analyzer, Optimizer, Planner, SQLConf"],
         ["sql(sqlText)","执行 SQL → parsePlan → executePlan → Dataset"],
         ["read → DataFrameReader","数据源读取：json/csv/parquet/jdbc"],
         ["catalog","元数据 Catalog：数据库/表/函数管理"]],
        [pw*0.25,pw*0.75]))

    E.append(Paragraph("10.2 Dataset/DataFrame —— 核心数据抽象", s['H2']))
    E.append(Paragraph("Dataset[T] 是 Spark SQL 最核心的数据抽象，DataFrame 即 Dataset[Row]。每个 Dataset 内部持有 QueryExecution，所有 Transformation 都是惰性的，Action 触发实际执行。", s['B']))
    E.append(mt(["API 类别","方法","说明"],
        [["Transformation","select/filter/join/groupBy/agg/sort/union","惰性转换，构建逻辑计划"],
         ["Action","collect/count/show/take/foreach/write","触发执行，返回结果"],
         ["I/O","read/write/save/insertInto","数据读写"],
         ["Typed","map/flatMap/mapPartitions","强类型转换"]],
        [pw*0.15,pw*0.4,pw*0.45]))

    E.append(Paragraph("10.3 SparkPlan —— 物理计划体系", s['H2']))
    E.append(mt(["类别","具体算子","说明"],
        [["基础算子","ProjectExec, FilterExec, SortExec, UnionExec","基础关系代数操作"],
         ["Join","BroadcastHashJoin, SortMergeJoin, ShuffledHashJoin","多种 Join 实现"],
         ["聚合","HashAggregateExec, SortAggregateExec, ObjectHashAggExec","三种聚合策略"],
         ["Exchange","ShuffleExchangeExec, BroadcastExchangeExec","数据重分布"],
         ["代码生成","WholeStageCodegenExec, InputAdapter","全阶段代码融合"],
         ["自适应","AdaptiveSparkPlanExec, QueryStageExec","AQE 运行时优化"],
         ["数据源","DataSourceScanExec, InMemoryTableScanExec","数据读取"]],
        [pw*0.13,pw*0.45,pw*0.42]))

    E.append(Paragraph("10.4 Expression —— 表达式体系", s['H2']))
    E.append(mt(["属性","类型","说明"],
        [["foldable","Boolean","是否可常量折叠"],
         ["deterministic","Boolean","相同输入是否始终相同输出"],
         ["nullable","Boolean","结果是否可能为 null"],
         ["dataType","DataType","结果数据类型"],
         ["eval(row)","Any","解释执行求值"],
         ["genCode(ctx)","ExprCode","代码生成（WholeStageCodegen）"],
         ["canonicalized","Expression","规范化形式（用于语义相等判断）"]],
        [pw*0.2,pw*0.15,pw*0.65]))
    E.append(PageBreak())

    # ======== 第十一章 ========
    E.append(Paragraph("第十一章 总结与架构评价", s['H1']))

    E.append(Paragraph("11.1 Spark SQL 架构的五大设计精髓", s['H2']))
    for t,d in [
        ("统一的树结构","TreeNode 作为万物之基，LogicalPlan/SparkPlan/Expression 共用相同的遍历和转换 API，极大简化了框架设计"),
        ("规则化转换引擎","每个分析/优化步骤封装为独立的 Rule，通过 RuleExecutor 的 Batch+Strategy 机制组合执行，易于扩展和调试"),
        ("不可变树 + 函数式转换","每次 transform 返回新树，保证线程安全和可推理性。结构共享机制保证性能"),
        ("策略模式的物理计划生成","SparkPlanner 的 18 个策略按优先级匹配，支持用户自定义扩展策略"),
        ("运行时自适应优化","AQE 突破静态优化的局限，利用运行时统计信息动态调整执行策略，显著提升复杂查询性能"),
    ]:
        E.append(Paragraph(f"<b>• {t}</b>：{d}", s['B']))

    E.append(Paragraph("11.2 关键数据流总结", s['H2']))
    E.append(mt(["维度","描述"],
        [["输入","SQL 字符串 / DataFrame API 链式调用"],
         ["解析输出","Unresolved LogicalPlan（含 UnresolvedRelation/Attribute）"],
         ["分析输出","Resolved LogicalPlan（所有引用已绑定）"],
         ["优化输出","Optimized LogicalPlan（下推/裁剪/折叠后）"],
         ["物理计划","SparkPlan（含具体物理算子选择）"],
         ["可执行计划","ExecutedPlan（满足分布/排序/codegen 需求）"],
         ["最终执行","RDD[InternalRow] → Spark Core 分布式执行"]],
        [pw*0.15,pw*0.85]))

    E.append(Paragraph("11.3 代码规模统计", s['H2']))
    E.append(mt(["模块","Scala 文件数","核心类数","最大文件","关键复杂度"],
        [["catalyst","350+","~500","AstBuilder(269KB)","Analyzer(4252行), Optimizer(2944行)"],
         ["core","200+","~300","Dataset(87KB)","SparkStrategies(53KB), QueryExecution(815行)"],
         ["hive","33","~50","HiveClientImpl(64KB)","类加载隔离+多版本兼容"],
         ["connect","67","~100","SparkConnectPlanner(170KB)","Proto ↔ Catalyst 全量转换"],
         ["pipelines","64","~80","SqlGraphRegCtx(689行)","图拓扑排序+流批统一"]],
        [pw*0.1,pw*0.12,pw*0.1,pw*0.28,pw*0.4]))

    E.append(Spacer(1,15*mm))
    E.append(Paragraph("— 全文完 —", s['Cap']))

    doc.build(E)
    print(f"\n✅ PDF 已生成: {out}")
    return out

if __name__ == "__main__":
    build_pdf()
