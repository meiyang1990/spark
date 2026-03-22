#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Spark Core 源码分析文档 PDF 生成脚本"""

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, PageBreak,
    Table, TableStyle, Flowable, HRFlowable
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
import math

pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))

# Colors
PRIMARY = HexColor('#1a237e')
SECONDARY = HexColor('#283593')
ACCENT = HexColor('#3949ab')
LIGHT_BG = HexColor('#e8eaf6')
CODE_BG = HexColor('#f5f5f5')
BORDER = HexColor('#c5cae9')
TEXT_COLOR = HexColor('#212121')
GRAY_TEXT = HexColor('#757575')
ORANGE = HexColor('#e65100')
GREEN = HexColor('#2e7d32')

def S():
    """Create all styles"""
    d = {}
    d['ct'] = ParagraphStyle('CT', fontName='STSong-Light', fontSize=32, textColor=PRIMARY, alignment=TA_CENTER, leading=44, spaceAfter=10)
    d['cs'] = ParagraphStyle('CS', fontName='STSong-Light', fontSize=16, textColor=SECONDARY, alignment=TA_CENTER, leading=24, spaceAfter=6)
    d['ci'] = ParagraphStyle('CI', fontName='STSong-Light', fontSize=12, textColor=GRAY_TEXT, alignment=TA_CENTER, leading=18)
    d['tt'] = ParagraphStyle('TT', fontName='STSong-Light', fontSize=22, textColor=PRIMARY, alignment=TA_CENTER, leading=30, spaceBefore=20, spaceAfter=20)
    d['ti'] = ParagraphStyle('TI', fontName='STSong-Light', fontSize=12, textColor=TEXT_COLOR, leading=22, leftIndent=20)
    d['ts'] = ParagraphStyle('TS', fontName='STSong-Light', fontSize=11, textColor=GRAY_TEXT, leading=20, leftIndent=45)
    d['h1'] = ParagraphStyle('H1', fontName='STSong-Light', fontSize=22, textColor=PRIMARY, leading=30, spaceBefore=24, spaceAfter=12)
    d['h2'] = ParagraphStyle('H2', fontName='STSong-Light', fontSize=16, textColor=SECONDARY, leading=22, spaceBefore=18, spaceAfter=8)
    d['h3'] = ParagraphStyle('H3', fontName='STSong-Light', fontSize=13, textColor=ACCENT, leading=18, spaceBefore=12, spaceAfter=6)
    d['b'] = ParagraphStyle('B', fontName='STSong-Light', fontSize=10, textColor=TEXT_COLOR, leading=16, spaceAfter=6, alignment=TA_JUSTIFY)
    d['bi'] = ParagraphStyle('BI', fontName='STSong-Light', fontSize=10, textColor=TEXT_COLOR, leading=16, spaceAfter=4, leftIndent=15, alignment=TA_JUSTIFY)
    d['bu'] = ParagraphStyle('BU', fontName='STSong-Light', fontSize=10, textColor=TEXT_COLOR, leading=16, spaceAfter=3, leftIndent=20, bulletIndent=8)
    d['nt'] = ParagraphStyle('NT', fontName='STSong-Light', fontSize=9.5, textColor=ORANGE, leading=15, spaceAfter=6, leftIndent=15, borderWidth=1, borderColor=ORANGE, borderPadding=6)
    d['cp'] = ParagraphStyle('CP', fontName='STSong-Light', fontSize=9, textColor=GRAY_TEXT, alignment=TA_CENTER, leading=14, spaceBefore=4, spaceAfter=10)
    return d

class FC(Flowable):
    def __init__(self, w, h, fn):
        Flowable.__init__(self)
        self.width = w
        self.height = h
        self.fn = fn
    def wrap(self, aw, ah):
        return (self.width, self.height)
    def draw(self):
        self.fn(self.canv, self.width, self.height)

def rr(c, x, y, w, h, fc=None, sc=BORDER):
    if fc: c.setFillColor(fc)
    c.setStrokeColor(sc); c.setLineWidth(0.8)
    c.roundRect(x, y, w, h, 4, stroke=1, fill=1 if fc else 0)

def arr(c, x1, y1, x2, y2, col=GRAY_TEXT):
    c.setStrokeColor(col); c.setFillColor(col); c.setLineWidth(1)
    c.line(x1, y1, x2, y2)
    a = math.atan2(y2-y1, x2-x1); L = 6
    p = c.beginPath()
    p.moveTo(x2, y2)
    p.lineTo(x2-L*math.cos(a-0.4), y2-L*math.sin(a-0.4))
    p.lineTo(x2-L*math.cos(a+0.4), y2-L*math.sin(a+0.4))
    p.close()
    c.drawPath(p, fill=1, stroke=0)

def bt(c, x, y, w, h, txt, fc=LIGHT_BG, tc=TEXT_COLOR, fs=8):
    rr(c, x, y, w, h, fc, BORDER)
    c.setFont('STSong-Light', fs); c.setFillColor(tc)
    lines = txt.split('\n')
    lh = fs + 2; th = len(lines) * lh; sy = y + h/2 + th/2 - fs
    for i, ln in enumerate(lines):
        tw = c.stringWidth(ln, 'STSong-Light', fs)
        c.drawString(x + (w - tw)/2, sy - i*lh, ln)

def tbl(data, cw):
    t = Table(data, colWidths=cw)
    t.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,-1), 'STSong-Light'),
        ('FONTSIZE', (0,0), (-1,0), 9), ('FONTSIZE', (0,1), (-1,-1), 8),
        ('BACKGROUND', (0,0), (-1,0), SECONDARY), ('TEXTCOLOR', (0,0), (-1,0), white),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [white, LIGHT_BG]),
        ('GRID', (0,0), (-1,-1), 0.5, BORDER),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 4), ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING', (0,0), (-1,-1), 5),
    ]))
    return t

# ===== Flow chart drawing functions =====

def draw_arch(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, 'Spark Core 整体架构')
    bt(cv, w/2-70, h-50, 140, 25, '用户应用 (Application)', PRIMARY, white, 9)
    arr(cv, w/2, h-50, w/2, h-65)
    bt(cv, w/2-80, h-95, 160, 28, 'SparkContext\n(应用入口 & 资源协调)', SECONDARY, white, 8)
    arr(cv, w/2-40, h-95, w/4-10, h-115); arr(cv, w/2, h-95, w/2, h-115); arr(cv, w/2+40, h-95, w*3/4+10, h-115)
    y = h-148
    bt(cv, 15, y, 130, 30, 'DAGScheduler\n(Stage划分&依赖分析)', HexColor('#c8e6c9'), TEXT_COLOR, 8)
    bt(cv, w/2-65, y, 130, 30, 'TaskScheduler\n(任务分配&本地性调度)', HexColor('#c8e6c9'), TEXT_COLOR, 8)
    bt(cv, w-145, y, 130, 30, 'SchedulerBackend\n(集群资源管理)', HexColor('#c8e6c9'), TEXT_COLOR, 8)
    arr(cv, 80, y+15, w/2-65, y+15); arr(cv, w/2+65, y+15, w-145, y+15)
    ye = y-48
    bt(cv, 15, ye, 100, 25, 'Executor\n(任务执行)', HexColor('#bbdefb'), TEXT_COLOR, 8)
    bt(cv, 125, ye, 100, 25, 'TaskRunner\n(线程执行)', HexColor('#bbdefb'), TEXT_COLOR, 8)
    bt(cv, 235, ye, 100, 25, 'BlockManager\n(存储管理)', HexColor('#ffe0b2'), TEXT_COLOR, 8)
    bt(cv, 345, ye, 120, 25, 'ShuffleManager\n(Shuffle读写)', HexColor('#ffe0b2'), TEXT_COLOR, 8)
    arr(cv, w/2, y, w/2, ye+25)
    yi = ye-45
    for i, (lbl, xi) in enumerate([('RPC(Netty)\n通信框架',15),('MemoryMgr\n内存管理',120),('Serializer\n序列化',225),('Metrics\n指标系统',330),('Broadcast\n广播变量',435)]):
        bt(cv, xi, yi, 95 if xi<435 else 50, 25, lbl, HexColor('#f3e5f5'), TEXT_COLOR, 7)

def draw_job(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, 'Job 提交与执行流程')
    boxes = [
        ('用户调用 Action (collect/count/save)', HexColor('#e3f2fd')),
        ('SparkContext.runJob() - 闭包清理&分区校验', HexColor('#e8f5e9')),
        ('DAGScheduler.submitJob() - 创建JobWaiter', HexColor('#e8f5e9')),
        ('handleJobSubmitted() - 创建ResultStage(递归)', HexColor('#fff3e0')),
        ('submitStage() - 递归提交: 先父后子', HexColor('#fff3e0')),
        ('submitMissingTasks() - 创建Task&序列化广播', HexColor('#fce4ec')),
        ('TaskScheduler.submitTasks() - 资源分配', HexColor('#fce4ec')),
        ('Executor 执行任务 - 结果返回 Driver', HexColor('#f3e5f5')),
    ]
    bh, gap, sy, bw = 28, 7, h-35, 260
    for i,(txt,col) in enumerate(boxes):
        y = sy - i*(bh+gap)
        bt(cv, w/2-bw/2, y, bw, bh, txt, col, TEXT_COLOR, 8)
        if i < len(boxes)-1:
            arr(cv, w/2, y, w/2, y-gap, ACCENT)

def draw_stage(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, 'Stage 划分与依赖分析')
    yt = h-45
    for x, name in [(40,'HadoopRDD'),(150,'MapPartRDD'),(280,'FilterRDD')]:
        bt(cv, x, yt, 95, 22, name, LIGHT_BG, TEXT_COLOR, 7)
    for x, name in [(160,'ShuffledRDD'),(280,'MapPartRDD'),(400,'ResultRDD')]:
        bt(cv, x, yt-55, 95, 22, name, LIGHT_BG, TEXT_COLOR, 7)
    arr(cv, 135, yt+11, 150, yt+11, GREEN); arr(cv, 245, yt+11, 280, yt+11, GREEN)
    cv.setStrokeColor(ORANGE); cv.setDash([3,3]); cv.line(330, yt, 210, yt-33); cv.setDash([])
    arr(cv, 255, yt-44, 280, yt-44, GREEN); arr(cv, 375, yt-44, 400, yt-44, GREEN)
    cv.setStrokeColor(GREEN); cv.setLineWidth(1.2); cv.setDash([5,3])
    cv.roundRect(30, yt-8, 360, 38, 5, stroke=1, fill=0)
    cv.setStrokeColor(ORANGE); cv.roundRect(150, yt-63, 360, 38, 5, stroke=1, fill=0)
    cv.setDash([])
    cv.setFont('STSong-Light', 8)
    cv.setFillColor(GREEN); cv.drawString(35, yt+32, 'Stage 0 (ShuffleMapStage)')
    cv.setFillColor(ORANGE); cv.drawString(155, yt-28, 'Stage 1 (ResultStage)')
    yd = yt-95
    cv.setFont('STSong-Light', 8); cv.setFillColor(TEXT_COLOR)
    cv.drawString(30, yd, '窄依赖(NarrowDep): 同Stage流水线执行    宽依赖(ShuffleDep): Stage边界,需Shuffle')

def draw_task(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, '任务调度与执行流程')
    y = h-45; c1, c2, c3 = 20, 185, 350
    cv.setFont('STSong-Light', 9); cv.setFillColor(PRIMARY)
    cv.drawString(c1, y+8, 'Driver端'); cv.drawString(c3, y+8, 'Executor端')
    bt(cv, c1, y-25, 140, 22, 'TaskSchedulerImpl\nsubmitTasks()', HexColor('#e8f5e9'), TEXT_COLOR, 8)
    arr(cv, c1+70, y-25, c1+70, y-55)
    bt(cv, c1, y-78, 140, 22, 'TaskSetManager\n注册到调度池', HexColor('#e8f5e9'), TEXT_COLOR, 8)
    arr(cv, c1+140, y-67, c2, y-67)
    bt(cv, c2, y-78, 140, 22, 'SchedulerBackend\nreviveOffers()', HexColor('#fff3e0'), TEXT_COLOR, 8)
    arr(cv, c2+70, y-78, c2+70, y-108)
    bt(cv, c2, y-130, 140, 22, 'resourceOffers()\n资源匹配&分配', HexColor('#fff3e0'), TEXT_COLOR, 8)
    arr(cv, c2+70, y-130, c2+70, y-160)
    ly = y-198
    bt(cv, c1, ly, 310, 35, '本地性: PROCESS_LOCAL>NODE_LOCAL>NO_PREF>RACK_LOCAL>ANY\n延迟调度: 高本地性等待后再降级', HexColor('#fce4ec'), TEXT_COLOR, 7)
    arr(cv, c2+70, ly, c3, ly+17)
    bt(cv, c3, ly, 130, 22, 'Executor.launchTask\n创建TaskRunner', HexColor('#e3f2fd'), TEXT_COLOR, 8)
    arr(cv, c3+65, ly, c3+65, ly-30)
    bt(cv, c3, ly-52, 130, 22, 'TaskRunner.run()\n反序列化&执行', HexColor('#e3f2fd'), TEXT_COLOR, 8)
    arr(cv, c3+65, ly-52, c3+65, ly-82)
    bt(cv, c3, ly-102, 130, 20, '结果序列化&汇报', HexColor('#f3e5f5'), TEXT_COLOR, 8)
    arr(cv, c3, ly-92, c1+140, ly-92, ACCENT)
    bt(cv, c1, ly-102, 140, 20, 'handleTaskCompletion\n触发下游Stage/Job完成', HexColor('#e8f5e9'), TEXT_COLOR, 7)

def draw_shuffle(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, 'Shuffle 三种写入路径')
    y = h-45
    bt(cv, w/2-80, y, 160, 22, 'SortShuffleManager\nregisterShuffle()', PRIMARY, white, 8)
    y2 = y-42; pw, ph = 145, 45
    bt(cv, 15, y2, pw, ph, 'Bypass Merge Sort\n无Map端聚合\n分区数<阈值(200)', HexColor('#e8f5e9'), TEXT_COLOR, 7)
    bt(cv, 170, y2, pw, ph, 'Tungsten序列化排序\n无聚合+序列化器\n支持重定位+分区<=16M', HexColor('#fff3e0'), TEXT_COLOR, 7)
    bt(cv, 325, y2, pw, ph, '反序列化排序(默认)\n其他所有情况\n含Map端聚合', HexColor('#fce4ec'), TEXT_COLOR, 7)
    arr(cv, w/2-50, y, 15+pw/2, y2+ph); arr(cv, w/2, y, 170+pw/2, y2+ph); arr(cv, w/2+50, y, 325+pw/2, y2+ph)
    y3 = y2-30
    arr(cv, 15+pw/2, y2, w/2, y3+22); arr(cv, 170+pw/2, y2, w/2, y3+22); arr(cv, 325+pw/2, y2, w/2, y3+22)
    bt(cv, w/2-100, y3, 200, 22, '输出: 数据文件 + 索引文件', HexColor('#e3f2fd'), TEXT_COLOR, 8)
    y4 = y3-35
    arr(cv, w/2, y3, w/2, y4+22)
    bt(cv, w/2-110, y4, 220, 22, 'BlockStoreShuffleReader\nMapOutputTracker定位数据', HexColor('#f3e5f5'), TEXT_COLOR, 8)

def draw_mem(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, '统一内存管理模型')
    tw, th = 460, 60; x0 = (w-tw)/2; y0 = h-90
    cv.setStrokeColor(PRIMARY); cv.setLineWidth(1.5)
    cv.roundRect(x0, y0, tw, th, 5, stroke=1, fill=0)
    cv.setFont('STSong-Light', 8); cv.setFillColor(PRIMARY); cv.drawString(x0+5, y0+th+3, 'JVM Heap')
    rw = 50; sw = tw - rw; mw = sw*0.6; ow = sw*0.4
    rr(cv, x0+2, y0+2, rw-4, th-4, HexColor('#ffcdd2'))
    cv.setFont('STSong-Light', 7); cv.setFillColor(TEXT_COLOR)
    cv.drawCentredString(x0+rw/2, y0+th/2+2, '保留内存'); cv.drawCentredString(x0+rw/2, y0+th/2-10, '300MB')
    ox = x0+rw
    rr(cv, ox+2, y0+2, ow-4, th-4, HexColor('#e0e0e0'))
    cv.drawCentredString(ox+ow/2, y0+th/2, '用户内存(40%)')
    mx = ox+ow; ew = mw/2
    rr(cv, mx+2, y0+2, ew-2, th-4, HexColor('#bbdefb'))
    cv.setFillColor(TEXT_COLOR)
    cv.drawCentredString(mx+ew/2, y0+th/2+8, '执行内存')
    cv.setFont('STSong-Light', 6); cv.drawCentredString(mx+ew/2, y0+th/2-6, 'Shuffle/Join/Sort')
    rr(cv, mx+ew+2, y0+2, ew-4, th-4, HexColor('#c8e6c9'))
    cv.setFont('STSong-Light', 7); cv.setFillColor(TEXT_COLOR)
    cv.drawCentredString(mx+ew+ew/2, y0+th/2+8, '存储内存')
    cv.setFont('STSong-Light', 6); cv.drawCentredString(mx+ew+ew/2, y0+th/2-6, 'Cache/Broadcast')
    mid = mx+ew
    cv.setStrokeColor(ORANGE); cv.setLineWidth(1.5); cv.setDash([3,2])
    cv.line(mid, y0+5, mid, y0+th-5); cv.setDash([])
    cv.setFont('STSong-Light', 7); cv.setFillColor(ORANGE); cv.drawCentredString(mid, y0-8, '软边界(可互借)')
    yf = y0-30
    cv.setFont('STSong-Light', 8); cv.setFillColor(TEXT_COLOR)
    cv.drawString(x0, yf, '可用内存 = (系统内存-300MB) x spark.memory.fraction(0.6)')
    cv.drawString(x0, yf-14, '存储区域 = 可用内存 x spark.memory.storageFraction(0.5)')
    cv.drawString(x0, yf-28, '执行区域 = 可用内存 - 存储区域')
    yr = yf-55
    cv.setFont('STSong-Light', 9); cv.setFillColor(PRIMARY); cv.drawString(x0, yr, '不对称借用规则:')
    cv.setFont('STSong-Light', 8); cv.setFillColor(TEXT_COLOR)
    cv.drawString(x0+10, yr-16, '1. 执行向存储借: 可强制驱逐存储中的缓存块(缓存可重算)')
    cv.drawString(x0+10, yr-30, '2. 存储向执行借: 只能借执行的空闲内存(不能驱逐执行内存)')

def draw_bm(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, 'BlockManager 存储与检索')
    lx, rx = 20, 225; bh, gap = 24, 8; y = h-55
    cv.setFont('STSong-Light', 9); cv.setFillColor(GREEN); cv.drawString(lx, h-38, 'Put流程(存储)')
    cv.setFillColor(ORANGE); cv.drawString(rx, h-38, 'Get流程(检索)')
    for i,(txt,col) in enumerate([('doPut()+BlockStoreUpdater',HexColor('#e8f5e9')),('尝试写入MemoryStore',HexColor('#bbdefb')),('内存不足降级DiskStore',HexColor('#ffe0b2')),('replication>1异步复制',HexColor('#f3e5f5')),('reportBlockStatus汇报',HexColor('#e8f5e9'))]):
        bt(cv, lx, y-i*(bh+gap), 165, bh, txt, col, TEXT_COLOR, 7)
        if i<4: arr(cv, lx+82, y-i*(bh+gap), lx+82, y-i*(bh+gap)-gap)
    for i,(txt,col) in enumerate([('get()入口',HexColor('#fff3e0')),('getLocalValues先内存后磁盘',HexColor('#bbdefb')),('未命中getRemoteValues',HexColor('#ffe0b2')),('优先同主机Executor直读',HexColor('#f3e5f5')),('Netty网络获取远程块',HexColor('#fce4ec'))]):
        bt(cv, rx, y-i*(bh+gap), 165, bh, txt, col, TEXT_COLOR, 7)
        if i<4: arr(cv, rx+82, y-i*(bh+gap), rx+82, y-i*(bh+gap)-gap)

def draw_fail(cv, w, h):
    cv.setFont('STSong-Light', 11); cv.setFillColor(PRIMARY)
    cv.drawCentredString(w/2, h-15, '故障处理与重试机制')
    y = h-42; cx = w/2
    bt(cv, cx-80, y, 160, 22, 'Task 执行失败', HexColor('#ffcdd2'), TEXT_COLOR, 8)
    arr(cv, cx, y, cx, y-15)
    y2 = y-38
    bt(cv, 15, y2, 110, 30, 'FetchFailed\nShuffle拉取失败', HexColor('#fff3e0'), TEXT_COLOR, 7)
    bt(cv, 135, y2, 110, 30, 'ExceptionFailure\n任务执行异常', HexColor('#e3f2fd'), TEXT_COLOR, 7)
    bt(cv, 255, y2, 110, 30, 'Barrier失败\n任一Task失败', HexColor('#f3e5f5'), TEXT_COLOR, 7)
    bt(cv, 375, y2, 100, 30, 'TaskKilled\n推测执行等', HexColor('#e0e0e0'), TEXT_COLOR, 7)
    arr(cv, cx-50, y, 70, y2+30); arr(cv, cx-15, y, 190, y2+30); arr(cv, cx+15, y, 310, y2+30); arr(cv, cx+50, y, 425, y2+30)
    y3 = y2-38
    bt(cv, 15, y3, 110, 22, '注销MapOutput\n延迟200ms重提交', HexColor('#c8e6c9'), TEXT_COLOR, 7)
    arr(cv, 70, y2, 70, y3+22)
    bt(cv, 135, y3, 110, 22, '递增失败次数\n重加待执行列表', HexColor('#bbdefb'), TEXT_COLOR, 7)
    arr(cv, 190, y2, 190, y3+22)
    bt(cv, 255, y3, 110, 22, 'Kill所有Task\n注销MapOutput重试', HexColor('#e1bee7'), TEXT_COLOR, 7)
    arr(cv, 310, y2, 310, y3+22)
    cv.setFont('STSong-Light', 7); cv.setFillColor(GRAY_TEXT)
    cv.drawString(130, y3+5, '>=4次abort'); cv.drawString(250, y3+5, '>=4次abort')

# ===== Build PDF =====
def build():
    out = '/Users/chenmeiyang/Documents/code/javaworkspace/github.com/meiyang1990/spark/Spark_Core_源码分析.pdf'
    doc = SimpleDocTemplate(out, pagesize=A4, leftMargin=2*cm, rightMargin=2*cm, topMargin=2.5*cm, bottomMargin=2*cm,
                            title='Apache Spark Core 源码深度分析', author='Source Code Analyst')
    s = S()
    st = []

    # Cover
    st.append(Spacer(1,80))
    st.append(Paragraph('Apache Spark Core', s['ct']))
    st.append(Paragraph('源码深度分析', s['ct']))
    st.append(Spacer(1,20))
    st.append(HRFlowable(width='60%', thickness=2, color=PRIMARY, spaceAfter=15, spaceBefore=5))
    st.append(Paragraph('核心流程图 & 关键类设计说明', s['cs']))
    st.append(Paragraph('基于 Spark 3.3.1 源码', s['cs']))
    st.append(Spacer(1,50))
    st.append(Paragraph('版本: v3.3.1  |  模块: spark-core', s['ci']))
    st.append(PageBreak())

    # TOC
    st.append(Paragraph('目  录', s['tt']))
    for t, subs in [
        ('第一章  Spark Core 整体架构概览', ['1.1 模块组织结构','1.2 整体架构图','1.3 核心组件关系']),
        ('第二章  核心流程图', ['2.1 Job提交与执行流程','2.2 Stage划分与依赖分析','2.3 任务调度与执行流程','2.4 Shuffle机制','2.5 统一内存管理','2.6 BlockManager存储','2.7 故障处理与重试']),
        ('第三章  核心类设计说明', ['3.1 SparkContext','3.2 DAGScheduler','3.3 TaskSchedulerImpl','3.4 Executor','3.5 RDD','3.6 BlockManager','3.7 UnifiedMemoryManager','3.8 SortShuffleManager','3.9 RPC框架','3.10 SparkEnv']),
        ('第四章  关键设计模式总结', []),
    ]:
        st.append(Paragraph(t, s['ti']))
        for sub in subs:
            st.append(Paragraph(sub, s['ts']))
    st.append(PageBreak())

    # Ch1
    st.append(Paragraph('第一章  Spark Core 整体架构概览', s['h1']))
    st.append(HRFlowable(width='100%', thickness=1.5, color=PRIMARY, spaceAfter=10))
    st.append(Paragraph('1.1 模块组织结构', s['h2']))
    st.append(Paragraph('Spark Core 是整个 Apache Spark 的基石, 位于 core/ 目录下, 提供分布式任务调度、内存管理、容错恢复、存储管理和 I/O 等核心能力。所有上层模块 (SQL、Streaming、MLlib、GraphX) 都构建在 Core 之上。', s['b']))
    st.append(tbl([
        ['模块目录','核心职责','关键类'],
        ['scheduler/','任务调度(DAG/Task)','DAGScheduler, TaskSchedulerImpl, TaskSetManager'],
        ['executor/','任务执行','Executor, TaskRunner'],
        ['rdd/','RDD抽象与实现','RDD, MapPartitionsRDD, ShuffledRDD'],
        ['storage/','块存储管理','BlockManager, MemoryStore, DiskStore'],
        ['shuffle/','Shuffle读写','SortShuffleManager, ShuffleWriter/Reader'],
        ['memory/','内存管理','UnifiedMemoryManager, MemoryPool'],
        ['rpc/','RPC通信','RpcEnv, RpcEndpoint, RpcEndpointRef'],
        ['deploy/','部署(Standalone)','Master, Worker, SparkSubmit'],
        ['broadcast/','广播变量','TorrentBroadcast, BroadcastManager'],
        ['serializer/','序列化','KryoSerializer, JavaSerializer'],
        ['util/','工具类','Utils, ClosureCleaner, EventLoop'],
    ], [70,140,260]))
    st.append(Spacer(1,12))
    st.append(Paragraph('1.2 整体架构图', s['h2']))
    st.append(FC(490, 250, draw_arch))
    st.append(Paragraph('图 1-1: Spark Core 分层架构', s['cp']))
    st.append(Paragraph('1.3 核心组件关系', s['h2']))
    st.append(Paragraph('Spark Core 的设计遵循分层架构, 从上到下分为四个层次:', s['b']))
    for x in ['<b>应用层</b>: SparkContext 作为统一入口, 管理应用生命周期','<b>调度层</b>: DAGScheduler(Stage级) + TaskScheduler(Task级)','<b>执行层</b>: Executor执行任务, BlockManager管理存储, ShuffleManager管理重分布','<b>基础设施层</b>: RPC通信、内存管理、序列化、指标系统']:
        st.append(Paragraph(x, s['bu'], bulletText='\xe2\x80\xa2'))
    st.append(PageBreak())

    # Ch2
    st.append(Paragraph('第二章  核心流程图', s['h1']))
    st.append(HRFlowable(width='100%', thickness=1.5, color=PRIMARY, spaceAfter=10))
    st.append(Paragraph('2.1 Job 提交与执行流程', s['h2']))
    st.append(Paragraph('当用户调用 RDD 的 Action 操作时, 会触发 Job 提交。流程从 SparkContext.runJob() 开始, 经 DAGScheduler 的 Stage 划分和 TaskScheduler 的任务分配, 最终在 Executor 上执行。', s['b']))
    st.append(FC(490, 310, draw_job))
    st.append(Paragraph('图 2-1: Job 从提交到执行的完整流程', s['cp']))
    st.append(Paragraph('2.2 Stage 划分与依赖分析', s['h2']))
    st.append(Paragraph('DAGScheduler 按 Shuffle 边界切分 Stage。窄依赖在同一 Stage 内流水线执行, 宽依赖触发 Stage 划分。', s['b']))
    st.append(FC(490, 180, draw_stage))
    st.append(Paragraph('图 2-2: Stage 划分与窄/宽依赖', s['cp']))
    st.append(PageBreak())
    st.append(Paragraph('2.3 任务调度与执行流程', s['h2']))
    st.append(Paragraph('任务调度涉及 Driver 端的 TaskSchedulerImpl/TaskSetManager 和 Executor 端的 Executor/TaskRunner 协作。延迟调度是性能优化的关键。', s['b']))
    st.append(FC(490, 310, draw_task))
    st.append(Paragraph('图 2-3: 任务调度与本地性优化', s['cp']))
    st.append(Paragraph('2.4 Shuffle 机制与三种写入路径', s['h2']))
    st.append(Paragraph('SortShuffleManager 根据条件选择三种写入路径。所有路径输出统一格式, 读取路径统一使用 BlockStoreShuffleReader。', s['b']))
    st.append(FC(490, 220, draw_shuffle))
    st.append(Paragraph('图 2-4: Shuffle 三种写入路径', s['cp']))
    st.append(PageBreak())
    st.append(Paragraph('2.5 统一内存管理模型', s['h2']))
    st.append(Paragraph('UnifiedMemoryManager 的核心特征是执行与存储内存之间的"软边界", 双方可互借但遵循不对称规则。', s['b']))
    st.append(FC(490, 250, draw_mem))
    st.append(Paragraph('图 2-5: 统一内存管理模型', s['cp']))
    st.append(Paragraph('2.6 BlockManager 存储与检索', s['h2']))
    st.append(Paragraph('BlockManager 管理 RDD 缓存、广播变量和 Shuffle 数据。存储"先内存后磁盘", 检索"先本地后远程"。', s['b']))
    st.append(FC(490, 210, draw_bm))
    st.append(Paragraph('图 2-6: BlockManager 存储与检索流程', s['cp']))
    st.append(Paragraph('2.7 故障处理与重试机制', s['h2']))
    st.append(Paragraph('不同失败类型有不同处理策略: FetchFailed 重新提交上游 Stage, ExceptionFailure 重试 Task, Barrier 失败重试整个 Stage。', s['b']))
    st.append(FC(490, 180, draw_fail))
    st.append(Paragraph('图 2-7: 故障分类与重试策略', s['cp']))
    st.append(PageBreak())

    # Ch3
    st.append(Paragraph('第三章  核心类设计说明', s['h1']))
    st.append(HRFlowable(width='100%', thickness=1.5, color=PRIMARY, spaceAfter=10))

    # 3.1 SparkContext
    st.append(Paragraph('3.1 SparkContext -- 应用入口', s['h2']))
    st.append(Paragraph('SparkContext 是 Spark 应用程序的主入口点(约3,250行), 负责初始化所有运行时组件并管理应用完整生命周期。', s['b']))
    st.append(Paragraph('核心职责:', s['h3']))
    for x in ['初始化约30个核心组件(SparkEnv、DAGScheduler、TaskScheduler等)','提供所有Action操作的入口(runJob/submitJob)','管理应用生命周期: 创建->运行->停止','通过工厂方法适配不同集群管理器(Local/Standalone/YARN/K8s)']:
        st.append(Paragraph(x, s['bu'], bulletText='\xe2\x80\xa2'))
    st.append(Paragraph('初始化四阶段:', s['h3']))
    st.append(tbl([['阶段','关键操作','核心组件'],['1.配置与基础','校验SparkConf,解析资源','_conf, _resources, _jars'],['2.事件与环境','创建运行时环境','ListenerBus, SparkEnv, SparkUI'],['3.调度器初始化','创建调度器和心跳','DAGScheduler, TaskScheduler, HeartbeatReceiver'],['4.启动与后置','启动调度器,初始化BlockMgr','BlockManager, MetricsSystem, EventLogger']],[85,170,210]))
    st.append(Spacer(1,6))
    st.append(Paragraph('runJob() 是所有 Action 的统一入口(7个重载), 核心流程: 检查未停止 -> 闭包清理 -> dagScheduler.runJob() -> RDD checkpoint。stop() 以严格逆序停止所有组件, AtomicBoolean 保证幂等。', s['b']))

    # 3.2 DAGScheduler
    st.append(Paragraph('3.2 DAGScheduler -- DAG 调度器', s['h2']))
    st.append(Paragraph('DAGScheduler(约3,625行) 是高层调度器, 将 RDD DAG 按 Shuffle 边界切分为 Stage, 根据依赖关系决定提交顺序。', s['b']))
    st.append(tbl([['数据结构','类型','用途'],['waitingStages','HashSet[Stage]','父Stage未完成,等待中'],['runningStages','HashSet[Stage]','当前正在运行'],['failedStages','HashSet[Stage]','需重新提交'],['stageIdToStage','HashMap','StageID到Stage映射'],['shuffleIdToMapStage','HashMap','ShuffleID到ShuffleMapStage映射']],[120,130,215]))
    st.append(Spacer(1,6))
    st.append(Paragraph('核心工作流: "自顶向下构建 Stage DAG -> 自底向上提交执行 -> 结果自底向上传播触发下游 -> 失败时回退重试"', s['nt']))

    # 3.3 TaskSchedulerImpl
    st.append(Paragraph('3.3 TaskSchedulerImpl -- 任务调度器', s['h2']))
    st.append(Paragraph('TaskSchedulerImpl(约1,308行) 是低层调度器, 将 Task 分配到具体 Executor。核心是 resourceOffers() 方法:', s['b']))
    for x in ['过滤排除节点(HealthTracker)','随机打乱Offers确保均衡','按策略排序TaskSet(FIFO/FAIR)','逐级本地性分配: PROCESS_LOCAL->NODE_LOCAL->NO_PREF->RACK_LOCAL->ANY','Barrier任务必须同一轮全部分配成功']:
        st.append(Paragraph(x, s['bu'], bulletText='\xe2\x80\xa2'))
    st.append(Paragraph('延迟调度: 在高本地性级别等待一段时间(spark.locality.wait.*), 等不到才降级, "牺牲公平性换数据本地性"。', s['b']))

    # 3.4 Executor
    st.append(Paragraph('3.4 Executor -- 任务执行器', s['h2']))
    st.append(Paragraph('Executor(约1,775行) 运行在 Worker 节点, 通过线程池并发执行 Task。TaskRunner.run() 流程:', s['b']))
    for x in ['环境准备: 创建TaskMemoryManager, 设置类加载器, 汇报RUNNING','依赖加载: 下载JAR/文件/归档(增量更新)','Task反序列化: 从Driver广播数据恢复Task对象','执行计算: task.run()触发RDD计算链','结果处理: 按大小三级策略(直接返回/存BlockManager/丢弃)']:
        st.append(Paragraph(x, s['bu'], bulletText='\xe2\x80\xa2'))
    st.append(tbl([['结果大小','策略','说明'],['>maxResultSize(1GB)','丢弃','仅返回IndirectTaskResult'],['>maxDirectResultSize','存BlockManager','返回间接引用'],['较小','直接RPC发送','序列化后Netty返回Driver']],[125,100,240]))

    st.append(PageBreak())

    # 3.5 RDD
    st.append(Paragraph('3.5 RDD -- 弹性分布式数据集', s['h2']))
    st.append(Paragraph('RDD(约2,710行) 是 Spark 最核心的数据抽象, 不可变、可分区、可并行计算。五大核心属性:', s['b']))
    st.append(tbl([['属性','方法','说明'],['分区列表','getPartitions','数据切分为多分区并行处理'],['计算函数','compute(split,ctx)','定义如何计算每个分区'],['依赖列表','getDependencies','对父RDD的依赖(容错血缘)'],['分区器','partitioner','K-V数据分区方式(可选)'],['首选位置','getPreferredLocations','数据本地化提示(可选)']],[70,135,260]))
    st.append(Spacer(1,6))
    st.append(Paragraph('依赖体系:', s['h3']))
    st.append(tbl([['依赖类型','子类','特征','示例操作'],['窄依赖','OneToOneDependency','子分区只依赖一个父分区','map, filter, flatMap'],['窄依赖','RangeDependency','连续分区范围映射','union'],['宽依赖','ShuffleDependency','父分区被多个子分区使用','reduceByKey, join']],[60,115,145,145]))
    st.append(Spacer(1,6))
    st.append(Paragraph('<b>MapPartitionsRDD</b>: 最常用的RDD(map/filter/flatMap底层), compute()对父RDD迭代器应用转换, 窄依赖流水线。<b>ShuffledRDD</b>: Shuffle产生的RDD(reduceByKey/join), compute()通过ShuffleReader从Map输出读取。', s['b']))

    # 3.6 BlockManager
    st.append(Paragraph('3.6 BlockManager -- 存储管理器', s['h2']))
    st.append(Paragraph('BlockManager(约2,400行) 运行在每个节点, 管理RDD缓存、广播变量和Shuffle数据, 协调MemoryStore和DiskStore两级存储。', s['b']))
    for x in ['<b>先内存后磁盘</b>: Put时优先MemoryStore, 不足降级DiskStore','<b>先本地后远程</b>: Get时先本地(内存->磁盘), 未命中再远程','<b>退役迁移</b>: 节点退役时BlockManagerDecommissioner迁移块','<b>读写锁</b>: BlockInfoManager提供块级读写锁机制']:
        st.append(Paragraph(x, s['bu'], bulletText='\xe2\x80\xa2'))

    # 3.7 UnifiedMemoryManager
    st.append(Paragraph('3.7 UnifiedMemoryManager -- 内存管理器', s['h2']))
    st.append(Paragraph('UnifiedMemoryManager(约493行) 的核心创新是"软边界": 执行和存储可互借, 但不对称 -- 执行可强制驱逐存储缓存(安全, 可重算), 存储只能借执行空闲内存(不能驱逐, 因需中断任务)。', s['b']))
    st.append(tbl([['场景','行为','原因'],['执行向存储借','可强制驱逐缓存块','缓存可重算恢复, 安全'],['存储向执行借','只能借空闲内存','驱逐执行内存需中断任务, 复杂']],[110,155,200]))

    # 3.8 SortShuffleManager
    st.append(Paragraph('3.8 SortShuffleManager -- Shuffle 管理器', s['h2']))
    st.append(Paragraph('SortShuffleManager(约273行) 唯一内置实现, 三种路径:', s['b']))
    st.append(tbl([['路径','条件','Writer'],['Bypass Merge Sort','无Map聚合+分区<200','BypassMergeSortShuffleWriter'],['Tungsten序列化排序','无聚合+序列化器支持重定位','UnsafeShuffleWriter'],['反序列化排序(默认)','其他所有(含Map聚合)','SortShuffleWriter']],[110,170,185]))
    st.append(Spacer(1,6))
    st.append(Paragraph('Tungsten优化: 二进制排序减少GC; 8字节指针提升缓存命中; 溢写合并无需反序列化; NIO transferTo高效复制。', s['b']))

    # 3.9 RPC
    st.append(Paragraph('3.9 RpcEnv & RpcEndpoint -- RPC 通信框架', s['h2']))
    st.append(Paragraph('基于 Netty 的 Endpoint/EndpointRef/RpcEnv 模式, 所有组件间通信都通过该框架完成。', s['b']))
    st.append(tbl([['组件','角色','关键方法'],['RpcEnv','消息总线/路由器','setupEndpoint, setupEndpointRef'],['RpcEndpoint','消息处理器','receive, receiveAndReply, onStart/Stop'],['RpcEndpointRef','远程引用/代理','send(单向), ask(请求-响应)']],[90,130,245]))

    # 3.10 SparkEnv
    st.append(Paragraph('3.10 SparkEnv -- 运行时环境', s['h2']))
    st.append(Paragraph('SparkEnv(约609行) 持有一个Spark实例所需全部运行时组件, Driver和Executor各自创建独立实例:', s['b']))
    st.append(tbl([['序号','组件','说明'],['1','SecurityManager','安全认证与加密'],['2','RpcEnv(Netty)','RPC通信环境'],['3','Serializer+SerializerManager','数据序列化'],['4','BroadcastManager','广播变量管理'],['5','MapOutputTracker','Map输出位置跟踪(Master/Worker)'],['6','BlockManagerMaster+BlockManager','分布式块存储管理'],['7','MetricsSystem','指标收集'],['8','OutputCommitCoordinator','输出提交协调']],[35,150,280]))

    st.append(PageBreak())

    # Ch4
    st.append(Paragraph('第四章  关键设计模式与思想总结', s['h1']))
    st.append(HRFlowable(width='100%', thickness=1.5, color=PRIMARY, spaceAfter=10))

    patterns = [
        ('1. 事件驱动架构', 'DAGScheduler 通过 EventLoop 处理所有调度事件, 单线程串行保证线程安全; LiveListenerBus 异步分发事件, 解耦核心逻辑与监控/UI。'),
        ('2. 分层调度设计', 'DAGScheduler(逻辑调度, Stage级) + TaskScheduler(物理调度, Task级) 的两层分离, 使得调度策略可以独立演进。'),
        ('3. 血缘容错机制', 'RDD 通过 Dependency 记录完整血缘, 分区丢失时从最近的完整数据重新计算, 无需数据复制, 存储开销极低。'),
        ('4. 软边界内存管理', '执行和存储内存共享同一空间, 通过不对称借用规则动态调整, 避免静态分配导致的资源浪费。'),
        ('5. 延迟调度优化', '在数据本地性和调度公平性之间取得平衡, 先在数据所在位置等待, 等不到再扩大搜索范围。'),
        ('6. 可插拔架构', 'ShuffleManager、Serializer、SchedulerBackend 等核心组件都可通过配置替换, 支持不同场景的定制。'),
        ('7. 推测执行容错', 'TaskSetManager 定期检查慢任务, 对运行时间超过中位数 1.5 倍的任务启动推测副本, 取先完成者结果。'),
        ('8. 多级结果返回', 'Executor 根据结果大小选择直接返回/存BlockManager/丢弃三级策略, 避免大结果打爆 Driver 内存。'),
        ('9. 异步复制存储', 'BlockManager 支持异步跨节点块复制和退役迁移, 提高数据可用性和集群弹性。'),
        ('10. 统一通信框架', '基于 Netty 的 RPC 框架统一了所有组件间通信, 支持单向消息和请求-响应两种模式, 简化了分布式系统开发。'),
    ]
    for title, desc in patterns:
        st.append(Paragraph(title, s['h3']))
        st.append(Paragraph(desc, s['b']))

    st.append(Spacer(1, 30))
    st.append(HRFlowable(width='100%', thickness=1, color=BORDER, spaceAfter=10))
    st.append(Paragraph('-- 文档结束 --', s['cp']))

    doc.build(st)
    print(f'PDF generated: {out}')

if __name__ == '__main__':
    build()
