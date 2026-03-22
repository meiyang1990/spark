#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Spark Resource Managers (K8s + YARN) 源码深度分析 PDF"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib.colors import HexColor, white
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus.flowables import Flowable
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
from reportlab.graphics import renderPDF
import os, platform, math

# ─── 颜色 ───
CP = HexColor('#1a56db'); CS = HexColor('#047857'); CA = HexColor('#dc2626')
CO = HexColor('#ea580c'); CV = HexColor('#7c3aed'); CT = HexColor('#0d9488')
CI = HexColor('#4338ca'); CK = HexColor('#db2777')
CLB = HexColor('#dbeafe'); CLG = HexColor('#d1fae5'); CLR = HexColor('#f3f4f6')
CLY = HexColor('#fef9c3'); CLP = HexColor('#ede9fe'); CLO = HexColor('#ffedd5')
CLPK = HexColor('#fce7f3'); CD = HexColor('#1f2937'); CB = HexColor('#d1d5db')
CK8 = HexColor('#326ce5'); CYN = HexColor('#f0ad4e')

def reg_font():
    ps = {
        'darwin': ['/System/Library/Fonts/STHeiti Medium.ttc','/System/Library/Fonts/PingFang.ttc',
                   '/Library/Fonts/Arial Unicode.ttf'],
        'linux': ['/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc'],
    }
    for fp in ps.get(platform.system().lower(), []):
        if os.path.exists(fp):
            try:
                pdfmetrics.registerFont(TTFont('CF', fp)); return 'CF'
            except: continue
    return 'Helvetica'

FN = reg_font()

# ─── 样式 ───
def mkst():
    d = {}
    d['ct'] = ParagraphStyle('CT',fontName=FN,fontSize=28,leading=38,alignment=TA_CENTER,textColor=CP,spaceAfter=8)
    d['cs'] = ParagraphStyle('CS',fontName=FN,fontSize=14,leading=22,alignment=TA_CENTER,textColor=CD,spaceAfter=4)
    d['h1'] = ParagraphStyle('H1',fontName=FN,fontSize=20,leading=28,textColor=CP,spaceBefore=18,spaceAfter=10)
    d['h2'] = ParagraphStyle('H2',fontName=FN,fontSize=15,leading=22,textColor=CS,spaceBefore=14,spaceAfter=8)
    d['h3'] = ParagraphStyle('H3',fontName=FN,fontSize=12,leading=18,textColor=CI,spaceBefore=10,spaceAfter=6)
    d['b'] = ParagraphStyle('B',fontName=FN,fontSize=9.5,leading=16,textColor=CD,spaceAfter=6,alignment=TA_JUSTIFY)
    d['bu'] = ParagraphStyle('BU',fontName=FN,fontSize=9.5,leading=16,textColor=CD,leftIndent=18,bulletIndent=6,spaceAfter=3)
    d['c'] = ParagraphStyle('C',fontName='Courier',fontSize=7.5,leading=11,textColor=HexColor('#1e293b'),
        backColor=CLR,leftIndent=12,rightIndent=12,spaceBefore=4,spaceAfter=4,borderPadding=6)
    d['cap'] = ParagraphStyle('CAP',fontName=FN,fontSize=9,leading=14,textColor=CP,alignment=TA_CENTER,spaceBefore=4,spaceAfter=10)
    return d

ST = mkst()

# ─── 绘图 ───
class DF(Flowable):
    def __init__(s, dw, w, h): Flowable.__init__(s); s.drawing=dw; s.width=w; s.height=h
    def wrap(s, aW, aH): return s.width, s.height
    def draw(s): renderPDF.draw(s.drawing, s.canv, 0, 0)

def bx(d,x,y,w,h,t,f=CLB,s=CP,tc=CD,fs=8,rx=4):
    d.add(Rect(x,y,w,h,rx=rx,ry=rx,fillColor=f,strokeColor=s,strokeWidth=0.8))
    ls=t.split('\n'); lh=fs+2; sy=y+h/2+len(ls)*lh/2-fs/2
    for i,l in enumerate(ls):
        d.add(String(x+w/2,sy-i*lh,l,fontName=FN,fontSize=fs,fillColor=tc,textAnchor='middle'))

def ar(d,x1,y1,x2,y2,c=CD,w=0.8):
    d.add(Line(x1,y1,x2,y2,strokeColor=c,strokeWidth=w))
    a=math.atan2(y2-y1,x2-x1); al=6; aa=math.pi/7
    d.add(Polygon([x2,y2,x2-al*math.cos(a-aa),y2-al*math.sin(a-aa),x2-al*math.cos(a+aa),y2-al*math.sin(a+aa)],
                  fillColor=c,strokeColor=c,strokeWidth=0.5))

def lb(d,x,y,t,c=CD,fs=7,an='middle'):
    d.add(String(x,y,t,fontName=FN,fontSize=fs,fillColor=c,textAnchor=an))

def tb(data,cw,hc=CP):
    t=Table(data,colWidths=cw)
    t.setStyle(TableStyle([('FONTNAME',(0,0),(-1,-1),FN),('FONTSIZE',(0,0),(-1,-1),7.5),
        ('BACKGROUND',(0,0),(-1,0),hc),('TEXTCOLOR',(0,0),(-1,0),white),
        ('GRID',(0,0),(-1,-1),0.5,CB),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),3)]))
    return t

# ═══════════════════════════════════════════
# 10 幅流程图
# ═══════════════════════════════════════════
def f1():
    """模块架构总览"""
    W,H=520,340; d=Drawing(W,H)
    lb(d,W/2,H-15,'Spark Resource Managers 模块架构总览',CP,12)
    bx(d,120,H-55,280,28,'Spark Core (ExternalClusterManager SPI)',CLR,CD,fs=9)
    bx(d,30,H-120,210,45,'Kubernetes 资源管理器\n(scheduler.cluster.k8s + deploy.k8s)',CLB,CK8,fs=9)
    bx(d,280,H-120,210,45,'YARN 资源管理器\n(scheduler.cluster + deploy.yarn)',CLO,CYN,fs=9)
    ar(d,210,H-55,135,H-75,CK8); ar(d,340,H-55,385,H-75,CYN)
    ky=H-190
    for xx,yy,t in [(10,ky,'submit/ Driver提交'),(120,ky,'features/ Pod配置链'),
        (10,ky-50,'Pod管理 Allocator+Life'),(120,ky-50,'事件 Watch+Poll+Store'),(65,ky-100,'shuffle/ PVC复用')]:
        bx(d,xx,yy,105,38,t,HexColor('#bfdbfe'),CK8,fs=7)
    ar(d,135,H-120,62,ky+38,CK8,0.6); ar(d,135,H-120,172,ky+38,CK8,0.6)
    for xx,yy,t in [(265,ky,'Client YARN提交76KB'),(380,ky,'ApplicationMaster AM'),
        (265,ky-50,'YarnAllocator 分配器'),(380,ky-50,'ExecutorRunnable 容器'),(320,ky-100,'安全/工具 RMClient')]:
        bx(d,xx,yy,108,38,t,HexColor('#fed7aa'),CYN,fs=7)
    ar(d,385,H-120,319,ky+38,CYN,0.6); ar(d,385,H-120,434,ky+38,CYN,0.6)
    bx(d,30,12,195,25,'Kubernetes API Server (fabric8)',HexColor('#e0e7ff'),CI,fs=8)
    bx(d,290,12,195,25,'YARN ResourceManager (AMRMClient)',HexColor('#fef3c7'),CO,fs=8)
    ar(d,117,ky-100,117,37,CK8,0.5); ar(d,374,ky-100,387,37,CYN,0.5)
    return DF(d,W,H)

def f2():
    """K8s Driver提交流程"""
    W,H=520,400; d=Drawing(W,H)
    lb(d,W/2,H-15,'Kubernetes Driver 提交流程 (Cluster Mode)',CK8,12)
    steps=[('spark-submit --master k8s://',CLR,CD),('KubernetesClientApplication.start()',CLB,CK8),
        ('生成appId: spark-<UUID>\n创建KubernetesDriverConf',CLB,CK8),
        ('KubernetesDriverBuilder\n.buildFromFeatures()',CLG,CS),('Feature Step链式配置(12个)',CLG,CS),
        ('创建K8s资源 Pod+Service+CM',CLP,CV),('LoggingPodStatusWatcher\n监视Driver Pod状态',CLO,CO)]
    bw,bh,sx,gap=190,34,30,48
    for i,(t,fl,s) in enumerate(steps):
        y=H-55-i*gap; bx(d,sx,y,bw,bh,t,fl,s,fs=7.5)
        if i<len(steps)-1: ar(d,sx+bw//2,y,sx+bw//2,y-gap+bh,s)
    fx,fy=260,H-100
    bx(d,fx,fy,240,22,'Feature Step 链 (foldLeft 串联)',CLG,CS,fs=8)
    feats=['BasicDriverFeatureStep (镜像/CPU/内存/端口)','DriverKubernetesCredentialsFeatureStep',
        'DriverServiceFeatureStep (Headless Service)','NetworkPolicyFeatureStep',
        'MountSecretsFeatureStep','EnvSecretsFeatureStep','MountVolumesFeatureStep',
        'DriverCommandFeatureStep (启动命令)','HadoopConfDriverFeatureStep',
        'KerberosConfDriverFeatureStep','PodTemplateConfigMapStep','LocalDirsFeatureStep (emptyDir)']
    for i,ft in enumerate(feats):
        bx(d,fx+5,fy-20-i*17,230,15,ft,white,CT,fs=6,rx=2)
    ar(d,sx+bw,H-55-3*gap+bh//2,fx,fy+11,CS,0.6)
    return DF(d,W,H)

def f3():
    """K8s事件驱动模型"""
    W,H=520,310; d=Drawing(W,H)
    lb(d,W/2,H-15,'Kubernetes Executor Pod 事件驱动模型',CK8,11)
    lb(d,90,H-45,'事件源(生产者)',CK8,8)
    bx(d,20,H-80,140,35,'WatchSnapshotSource\n(K8s Watch API增量)',CLB,CK8,fs=7.5)
    bx(d,20,H-130,140,35,'PollingSnapshotSource\n(定期全量轮询)',CLB,CK8,fs=7.5)
    lb(d,260,H-45,'快照存储',CO,8)
    bx(d,190,H-110,140,50,'SnapshotsStoreImpl\nupdatePod(增量)\nreplaceSnapshot(全量)\n时间窗口批量通知',CLY,CO,fs=7)
    ar(d,160,H-62,190,H-72,CK8); ar(d,160,H-112,190,H-100,CK8)
    lb(d,430,H-45,'订阅者(消费者)',CS,8)
    bx(d,360,H-80,140,35,'ExecutorPodsAllocator\n创建/删除Pod',CLG,CS,fs=7.5)
    bx(d,360,H-130,140,35,'PodsLifecycleManager\n处理Pod状态变更',CLG,CS,fs=7.5)
    ar(d,330,H-75,360,H-65,CS); ar(d,330,H-95,360,H-115,CS)
    ay=H-180
    bx(d,20,ay,220,105,'',HexColor('#f0fdf4'),CS)
    lb(d,130,ay+93,'Allocator.onNewSnapshots()',CS,7.5)
    for i,s in enumerate(['1. 聚合snapshots->获取K8s已知executor集合','2. 清理已确认的newlyCreated',
        '3. 检测超时Pod->删除重新调度','4. 按ResourceProfile分组统计',
        '5. 计算missing=target-podCount','6. requestNewExecutors()->build+create']):
        lb(d,28,ay+76-i*14,s,CD,6.5,'start')
    bx(d,270,ay,230,105,'',HexColor('#fef3c7'),CO)
    lb(d,385,ay+93,'LifecycleManager.onNewSnapshots()',CO,7.5)
    for i,s in enumerate(['1. 遍历Pod状态:','   PodDeleted->removeExecutorFromSpark',
        '   PodFailed->removeExecutor+删除+记录','   PodSucceeded->removeExecutor+删除',
        '2. 丢失Pod检测:对比scheduler与K8s','3. 失败次数>max->sys.exit()']):
        lb(d,278,ay+76-i*14,s,CD,6.5,'start')
    return DF(d,W,H)

def f4():
    """K8s类继承体系"""
    W,H=520,340; d=Drawing(W,H)
    lb(d,W/2,H-15,'Kubernetes 模块核心类继承体系',CK8,12)
    bx(d,30,H-50,145,22,'ExternalClusterManager',CLR,CD,fs=7.5)
    bx(d,30,H-82,145,22,'KubernetesClusterManager',CLB,CK8,fs=7.5)
    ar(d,102,H-50,102,H-60,CD)
    bx(d,210,H-50,175,22,'CoarseGrainedSchedulerBackend',CLR,CD,fs=7)
    bx(d,210,H-82,175,22,'K8sClusterSchedulerBackend',CLB,CK8,fs=7.5)
    ar(d,297,H-50,297,H-60,CD)
    bx(d,230,H-112,135,20,'KubernetesDriverEndpoint',HexColor('#bfdbfe'),CK8,fs=7)
    ar(d,297,H-82,297,H-92,CK8)
    bx(d,30,H-155,130,22,'AbstractPodsAllocator',CLY,CO,fs=7.5)
    for i,(t,xx) in enumerate([('ExecutorPodsAllocator\n(默认:直接Pod)',0),('StatefulSetAllocator',115),('DeploymentAllocator',230)]):
        bx(d,xx,H-190,108,20,t,CLY,CO,fs=6.5)
    ar(d,60,H-155,54,H-170,CO,0.5); ar(d,95,H-155,169,H-170,CO,0.5); ar(d,130,H-155,284,H-170,CO,0.5)
    bx(d,370,H-155,130,22,'KubernetesConf',CLP,CV,fs=7.5)
    bx(d,350,H-190,75,20,'DriverConf',CLP,CV,fs=7)
    bx(d,435,H-190,75,20,'ExecutorConf',CLP,CV,fs=7)
    ar(d,410,H-155,387,H-170,CV,0.5); ar(d,450,H-155,472,H-170,CV,0.5)
    bx(d,30,H-235,170,22,'KubernetesFeatureConfigStep (trait)',CLG,CS,fs=7.5)
    fl=['BasicDriverFS','DriverCommandFS','DriverServiceFS','MountSecretsFS','MountVolumesFS','LocalDirsFS']
    fr=['BasicExecutorFS','DriverK8sCredFS','NetworkPolicyFS','EnvSecretsFS','HadoopConfFS','PodTemplateCMFS']
    for i,ft in enumerate(fl): bx(d,10,H-262-i*16,130,14,ft,white,CT,fs=6,rx=2)
    for i,ft in enumerate(fr): bx(d,150,H-262-i*16,130,14,ft,white,CT,fs=6,rx=2)
    ar(d,115,H-235,75,H-248,CS,0.5); ar(d,115,H-235,215,H-248,CS,0.5)
    bx(d,350,H-235,140,22,'ExecutorPodState',CLPK,CK,fs=7.5)
    for i,s in enumerate(['PodRunning','PodPending','PodUnknown','PodSucceeded','PodFailed','PodDeleted','PodTerminating']):
        bx(d,360,H-262-i*15,115,13,s,white,CK,fs=5.5,rx=2)
    ar(d,420,H-235,420,H-249,CK,0.5)
    return DF(d,W,H)

def f5():
    """YARN Client Mode"""
    W,H=520,410; d=Drawing(W,H)
    lb(d,W/2,H-15,'YARN Client Mode 完整流程',CYN,12)
    dx,dy=20,H-50
    lb(d,dx+80,dy+12,'Driver进程(用户端)',CD,8)
    for i,(t,fl,s) in enumerate([('SparkContext.init()',CLR,CD),
        ('YarnClientSchedulerBackend.start()',CLO,CYN),
        ('Client.submitApplication()\n-> RM分配appId',CLO,CYN),
        ('waitForApplication()\n轮询直到RUNNING',CLO,CYN),
        ('asyncMonitorApplication()\n后台监控',CLO,CYN)]):
        y=dy-i*38; bx(d,dx,y,165,28,t,fl,s,fs=7.5)
        if i<4: ar(d,dx+82,y,dx+82,y-10,s if i>0 else CD)
    ax,ay=285,dy-55
    lb(d,ax+100,ay+45,'YARN集群(AM容器)',CO,8)
    for i,(t,fl,s) in enumerate([('ApplicationMaster\n(ExecutorLauncher模式)',CLY,CO),
        ('runExecutorLauncher()\nregisterAM(host,-1)',CLY,CO),
        ('连接Driver YarnSchedulerEndpoint\nsend(RegisterClusterManager)',CLY,CO),
        ('createAllocator()\n创建YarnAllocator',CLG,CS),
        ('launchReporterThread()\n循环allocateResources()',CLG,CS)]):
        y=ay-i*38; bx(d,ax,y,200,28,t,fl,s,fs=7)
        if i<4: ar(d,ax+100,y,ax+100,y-10,s)
    ar(d,dx+165,dy-76+14,ax,ay+14,CA,0.8); lb(d,228,ay+28,'提交AM',CA,7)
    ar(d,ax,ay-76+14,dx+165,dy-152+14,CV,0.8); lb(d,228,ay-70,'RPC注册',CV,7)
    ey=ay-5*38-10
    bx(d,ax+15,ey,170,26,'ExecutorRunnable.run()\n-> NMClient.startContainer()',CLB,CP,fs=7)
    ar(d,ax+100,ay-4*38,ax+100,ey+26,CP)
    bx(d,ax+15,ey-35,170,26,'YarnCoarseGrainedExecutor\n-> 连接Driver注册',CLB,CP,fs=7)
    ar(d,ax+100,ey,ax+100,ey-9,CP)
    return DF(d,W,H)

def f6():
    """YARN Cluster Mode"""
    W,H=520,420; d=Drawing(W,H)
    lb(d,W/2,H-15,'YARN Cluster Mode 完整流程',CO,12)
    sx,sy=15,H-50
    lb(d,sx+75,sy+12,'spark-submit(客户端)',CD,8)
    bx(d,sx,sy,150,24,'Client.main()',CLR,CD,fs=8)
    bx(d,sx,sy-35,150,24,'submitApplication()\namClass=ApplicationMaster',CLO,CYN,fs=7)
    bx(d,sx,sy-70,150,24,'monitorApplication()\n轮询(可选)',CLO,CYN,fs=7)
    ar(d,sx+75,sy,sx+75,sy-11,CD); ar(d,sx+75,sy-35,sx+75,sy-46,CYN)
    ax,ay=225,sy-5
    lb(d,ax+125,ay+18,'YARN集群(AM=Driver)',CO,8)
    for i,(t,fl,s) in enumerate([('ApplicationMaster.run()->runDriver()',CLY,CO),
        ('startUserApplication()->userClass.main()\n独立线程启动用户代码',CLY,CO),
        ('SparkContext初始化\nsparkContextInitialized()->promise',CLG,CS),
        ('registerAM(driverHost,driverPort)',CLY,CO),
        ('createAllocator(driverRef)\nYarnAllocator+首次分配',CLG,CS),
        ('resumeDriver()唤醒用户线程\nuserClassThread.join()',CLY,CO)]):
        y=ay-i*35; bx(d,ax,y,270,26,t,fl,s,fs=7)
        if i<5: ar(d,ax+135,y,ax+135,y-9,s)
    ar(d,sx+150,sy-22,ax,ay+13,CA,0.8); lb(d,195,ay+14,'提交',CA,7)
    ry=ay-6*35-10
    bx(d,ax+10,ry,250,24,'Reporter Thread: 循环allocateResources()',CLG,CS,fs=7.5)
    bx(d,ax+10,ry-30,250,24,'handleAllocatedContainers() 三级匹配',CLB,CP,fs=7)
    bx(d,ax+10,ry-60,250,24,'ExecutorRunnable->NMClient.startContainer()',CLB,CP,fs=7)
    ar(d,ax+135,ay-5*35,ax+135,ry+24,CS)
    ar(d,ax+135,ry,ax+135,ry-6,CP); ar(d,ax+135,ry-30,ax+135,ry-36,CP)
    bx(d,15,ry-60,180,24,'Executor->RegisterExecutor\n连接Driver RPC',HexColor('#bfdbfe'),CI,fs=7)
    ar(d,ax+10,ry-48,195,ry-48,CI,0.6)
    return DF(d,W,H)

def f7():
    """YARN Allocator"""
    W,H=520,380; d=Drawing(W,H)
    lb(d,W/2,H-15,'YarnAllocator 资源分配核心流程',CS,12)
    sx,sy,bw,bh,gap=15,H-50,175,28,40
    lb(d,sx+bw//2,sy+12,'allocateResources() 主循环',CD,8)
    for i,(t,fl,s) in enumerate([('updateResourceRequests()\n根据目标数更新容器请求',CLO,CO),
        ('amClient.allocate(0.1f)\n心跳+获取分配',CLY,CO),
        ('handleAllocatedContainers()\n处理已分配容器',CLG,CS),
        ('processCompletedContainers()\n处理已完成容器',CLB,CP)]):
        y=sy-i*gap; bx(d,sx,y,bw,bh,t,fl,s,fs=7.5)
        if i<3: ar(d,sx+bw//2,y,sx+bw//2,y-gap+bh,s)
    rx,ry=220,H-50
    bx(d,rx,ry,280,150,'',HexColor('#f0fdf4'),CS)
    lb(d,rx+140,ry+138,'handleAllocatedContainers() 三级匹配',CS,8)
    for i,(t,desc) in enumerate([('1. 按主机名匹配(NODE_LOCAL)','对每个容器查找同host的pending请求'),
        ('2. 按机架匹配(RACK_LOCAL)','独立线程解析机架并匹配'),
        ('3. 按ANY匹配(OFF_RACK)','剩余容器匹配任意pending请求'),
        ('4. 释放多余容器','amClient.releaseAssignedContainer()'),
        ('5. runAllocatedContainers()','在launcherPool线程池异步启动')]):
        lb(d,rx+8,ry+112-i*26,t,CS,7,'start'); lb(d,rx+12,ry+100-i*26,desc,CD,6.5,'start')
    ar(d,sx+bw,sy-2*gap+bh//2,rx,ry+75,CS,0.6)
    py=ry-170
    bx(d,rx,py,280,115,'',HexColor('#eff6ff'),CP)
    lb(d,rx+140,py+103,'processCompletedContainers() 退出状态',CP,8)
    for i,c in enumerate(['SUCCESS -> 非应用错误(preemption等)','PREEMPTED -> 被抢占,不算失败',
        'KILLED_EXCEEDED_VMEM -> 虚拟内存超限','KILLED_EXCEEDED_PMEM -> 物理内存超限',
        '其他 -> 检查坏节点,更新排除列表','-> driverRef.send(RemoveExecutor(eid, reason))']):
        lb(d,rx+8,py+85-i*15,c,CD,6.5,'start')
    ar(d,sx+bw,sy-3*gap+bh//2,rx,py+55,CP,0.6)
    return DF(d,W,H)

def f8():
    """YARN类继承"""
    W,H=520,330; d=Drawing(W,H)
    lb(d,W/2,H-15,'YARN 模块核心类继承体系',CYN,12)
    bx(d,185,H-48,150,22,'ExternalClusterManager(SPI)',CLR,CD,fs=7.5)
    bx(d,195,H-78,130,22,'YarnClusterManager',CLO,CYN,fs=7.5)
    ar(d,260,H-48,260,H-56,CD)
    bx(d,10,H-118,120,22,'TaskSchedulerImpl',CLR,CD,fs=7.5)
    bx(d,10,H-150,120,22,'YarnScheduler',CLO,CYN,fs=7)
    bx(d,10,H-182,120,22,'YarnClusterScheduler',CLO,CYN,fs=7)
    ar(d,70,H-118,70,H-128,CD); ar(d,70,H-150,70,H-160,CYN)
    bx(d,170,H-118,175,22,'CoarseGrainedSchedulerBackend',CLR,CD,fs=7)
    bx(d,180,H-150,155,22,'YarnSchedulerBackend\n(+YarnSchedulerEndpoint)',CLO,CYN,fs=6.5)
    bx(d,155,H-188,95,22,'YarnClient\nBackend',CLO,CYN,fs=6.5)
    bx(d,260,H-188,95,22,'YarnCluster\nBackend',CLO,CYN,fs=6.5)
    ar(d,257,H-118,257,H-128,CD)
    ar(d,230,H-150,202,H-166,CYN,0.6); ar(d,280,H-150,307,H-166,CYN,0.6)
    bx(d,385,H-118,120,22,'ApplicationMaster',CLY,CO,fs=7.5)
    bx(d,385,H-152,120,22,'YarnAllocator(49KB)',CLG,CS,fs=7.5)
    bx(d,385,H-186,120,22,'YarnRMClient',CLB,CP,fs=7.5)
    ar(d,445,H-118,445,H-130,CO); ar(d,445,H-152,445,H-164,CS)
    bx(d,10,H-228,115,22,'Client(76KB)\nYARN提交',HexColor('#fed7aa'),CO,fs=7)
    bx(d,135,H-228,115,22,'ExecutorRunnable\nNMClient启动',CLB,CP,fs=7)
    bx(d,260,H-228,125,22,'LocalityPreferred\nPlacement',CLG,CS,fs=6.5)
    bx(d,395,H-228,110,22,'NodeHealth\nTracker',CLPK,CK,fs=7)
    bx(d,10,H-300,495,50,'',HexColor('#faf5ff'),CV)
    lb(d,257,H-260,'Client Mode vs Cluster Mode 关键区别',CV,8)
    lb(d,18,H-275,'Client: Driver在客户端 | AM=ExecutorLauncher | YarnClientSchedulerBackend | spark.yarn.am.memory',CD,6.5,'start')
    lb(d,18,H-290,'Cluster: Driver在AM中 | AM=ApplicationMaster | YarnClusterSchedulerBackend | spark.driver.memory',CD,6.5,'start')
    return DF(d,W,H)

def f9():
    """本地性策略"""
    W,H=520,280; d=Drawing(W,H)
    lb(d,W/2,H-15,'YARN 本地性偏好容器放置策略',CS,11)
    for i,(t,fl,s) in enumerate([('输入: numContainers, hostToTaskCount\n计算每个host的任务比例',CLY,CO),
        ('expectedHostToContainerCount\n= 比例x总数 - 已分配 - 已pending',CLO,CO),
        ('分两组: localityFree + localityAware',CLG,CS),
        ('对localityAware容器:\n按比例轮询分配(nodes[],racks[])',CLB,CP),
        ('输出: 每个容器的\n(preferredNodes, preferredRacks)',CLP,CV)]):
        y=H-60-i*45; bx(d,15,y,225,32,t,fl,s,fs=7.5)
        if i<4: ar(d,127,y,127,y-13,CD)
    rx=270
    bx(d,rx,H-60,230,205,'',HexColor('#fffbeb'),CO)
    lb(d,rx+115,H-50,'示例',CO,9)
    for i,l in enumerate(['假设4个host的任务分布:','  host1:30  host2:30  host3:20  host4:10','',
        '请求15个容器时按比例:','  host1: 33% -> 5个','  host2: 33% -> 5个',
        '  host3: 22% -> 3个','  host4: 11% -> 2个','',
        '轮询分配偏好:','  容器1 -> prefer host1','  容器2 -> prefer host2',
        '  容器3 -> prefer host1','  容器4 -> prefer host2','  ...(递减直到ratio为0)']):
        lb(d,rx+10,H-73-i*12,l,CD,6.5,'start')
    return DF(d,W,H)

def f10():
    """K8s vs YARN对比"""
    W,H=520,300; d=Drawing(W,H)
    lb(d,W/2,H-15,'Kubernetes vs YARN 架构对比',CD,12)
    kx=15
    bx(d,kx,H-45,230,22,'Kubernetes 资源管理',CLB,CK8,fs=9)
    for i,(k,v) in enumerate([('资源单位','Pod (fabric8 K8s Client)'),('Pod构建','Feature Step链式(12步)'),
        ('分配策略','Direct/StatefulSet/Deployment'),('事件模型','Watch(增量)+Polling(全量)'),
        ('快照存储','SnapshotsStore(时间窗口)'),('Shuffle','PVC复用+数据恢复'),
        ('扩展','ExecutorRoll+ExecutorResize'),('服务发现','Headless Service+Label')]):
        y=H-75-i*27; bx(d,kx,y,75,21,k,HexColor('#bfdbfe'),CK8,fs=6.5); bx(d,kx+80,y,145,21,v,white,CB,fs=6.5)
    yx=275
    bx(d,yx,H-45,230,22,'YARN 资源管理',CLO,CYN,fs=9)
    for i,(k,v) in enumerate([('资源单位','Container (AMRMClient)'),('容器构建','createContainerLaunchCtx'),
        ('分配策略','AMRMClient心跳分配'),('事件模型','allocate()心跳轮询'),
        ('本地性','三级:Host->Rack->ANY'),('安全','Kerberos+Token续期'),
        ('部署模式','Client+Cluster Mode'),('容器启动','NMClient+ExecutorRunnable')]):
        y=H-75-i*27; bx(d,yx,y,75,21,k,HexColor('#fed7aa'),CYN,fs=6.5); bx(d,yx+80,y,145,21,v,white,CB,fs=6.5)
    lb(d,W/2,H-170,'VS',CA,16)
    return DF(d,W,H)

# ═══════════════════════════════════════════
# PDF文档构建
# ═══════════════════════════════════════════
def build():
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Spark_Resource_Managers_源码深度分析.pdf')
    doc = SimpleDocTemplate(out, pagesize=A4, topMargin=2*cm, bottomMargin=2*cm, leftMargin=2*cm, rightMargin=2*cm)
    E = []

    # 封面
    E.append(Spacer(1,80))
    E.append(Paragraph('Spark Resource Managers',ST['ct']))
    E.append(Paragraph('源码深度分析',ST['ct']))
    E.append(Spacer(1,20))
    E.append(Paragraph('Kubernetes + YARN 资源管理器架构与实现',ST['cs']))
    E.append(Spacer(1,15))
    E.append(tb([['模块','核心文件数','代码规模','关键设计'],
        ['Kubernetes','~40 Scala','~6000行','Feature Step链+事件驱动Pod管理'],
        ['YARN','~25 Scala+4 Java','~7000行','AM双模式+三级本地性匹配']],[80,95,80,205]))
    E.append(Spacer(1,25))
    E.append(Paragraph('基于 Apache Spark v3.3.1 源码分析',ST['cs']))
    E.append(PageBreak())

    # 目录
    E.append(Paragraph('目录',ST['h1']))
    for item in ['第一章  模块架构总览','第二章  Kubernetes 资源管理器',
        '  2.1 Driver提交流程','  2.2 Feature Step链式配置','  2.3 Executor Pod事件驱动模型',
        '  2.4 Pod分配器策略','  2.5 核心类继承体系','  2.6 Shuffle与PVC复用','  2.7 插件扩展',
        '第三章  YARN 资源管理器','  3.1 Client Mode完整流程','  3.2 Cluster Mode完整流程',
        '  3.3 YarnAllocator资源分配','  3.4 本地性匹配策略','  3.5 核心类继承体系','  3.6 AM与RM交互',
        '第四章  K8s vs YARN对比','第五章  核心设计精髓']:
        E.append(Paragraph(item,ParagraphStyle('T',fontName=FN,fontSize=10,leading=20,textColor=CD,leftIndent=18 if item.startswith('  ') else 0)))
    E.append(PageBreak())

    # ═══ 第一章 ═══
    E.append(Paragraph('第一章  模块架构总览',ST['h1']))
    E.append(Paragraph('Spark Resource Managers 模块是 Spark 与外部集群管理系统的桥梁，通过 <b>ExternalClusterManager</b> SPI 接口实现可插拔的资源管理。当前版本支持 <b>Kubernetes</b> 和 <b>YARN</b> 两大资源管理器，分别适用于容器化云原生和 Hadoop 生态部署场景。',ST['b']))
    E.append(Paragraph('图 1-1: 模块架构总览图',ST['cap']))
    E.append(f1())
    E.append(Paragraph('1.1 共同架构模式',ST['h2']))
    E.append(Paragraph('两个资源管理器虽面向不同底层平台，但共享相同的 Spark 调度框架：',ST['b']))
    E.append(tb([['架构层次','Kubernetes实现','YARN实现'],
        ['SPI入口','KubernetesClusterManager','YarnClusterManager'],
        ['调度后端','K8sClusterSchedulerBackend\n(继承CoarseGrainedSchedulerBackend)','YarnSchedulerBackend\n(Client/Cluster两种子类)'],
        ['资源申请','K8s API (fabric8) 创建Pod','AMRMClient.allocate() 申请Container'],
        ['Executor启动','K8s创建Pod->容器自启动','NMClient.startContainer()'],
        ['Executor监控','事件驱动(Watch+Polling)','AMRMClient心跳轮询'],
        ['资源配置','Feature Step链式(12步)','Client.createContainerLaunchContext()']],[80,190,190]))
    E.append(PageBreak())

    # ═══ 第二章 K8s ═══
    E.append(Paragraph('第二章  Kubernetes 资源管理器',ST['h1']))
    E.append(Paragraph('Spark on Kubernetes 模块位于 <font face="Courier">resource-managers/kubernetes/core</font>，约40个Scala文件、~6000行代码。采用 <b>Feature Step 链式配置</b> 和 <b>生产者-消费者事件驱动</b> 两大核心设计模式。',ST['b']))
    E.append(Paragraph('2.1 Driver 提交流程 (Cluster Mode)',ST['h2']))
    E.append(Paragraph('Cluster Mode下，<font face="Courier">spark-submit</font> 通过 <b>KubernetesClientApplication</b> 将Driver Pod提交到K8s集群。核心入口是 <font face="Courier">Client.run()</font>，使用 <b>KubernetesDriverBuilder</b> 构建完整的Driver Pod规格。提交流程为：解析参数 -> 生成appId(spark-UUID) -> 创建K8sClient -> 构建Feature Step链 -> 创建Pod/Service/ConfigMap -> 监视Pod状态。',ST['b']))
    E.append(Paragraph('图 2-1: Kubernetes Driver 提交流程',ST['cap']))
    E.append(f2())

    E.append(Paragraph('2.2 Feature Step 链式配置',ST['h2']))
    E.append(Paragraph('<b>KubernetesFeatureConfigStep</b> 是核心trait，定义了4个方法：configurePod(修改Pod规格)、getAdditionalPodSystemProperties(JVM属性)、getAdditionalPreKubernetesResources(Pod前资源)、getAdditionalKubernetesResources(Pod后资源)。所有Step通过 <font face="Courier">foldLeft</font> 串联，每步输出是下步输入。用户可通过配置注入自定义Step实现Sidecar注入、自定义Volume等。',ST['b']))
    code='trait KubernetesFeatureConfigStep {\n  def configurePod(pod: SparkPod): SparkPod\n  def getAdditionalPodSystemProperties(): Map[String, String]\n  def getAdditionalPreKubernetesResources(): Seq[HasMetadata]\n  def getAdditionalKubernetesResources(): Seq[HasMetadata]\n}'
    E.append(Paragraph(code.replace('\n','<br/>').replace(' ','&nbsp;'),ST['c']))
    E.append(PageBreak())

    E.append(Paragraph('2.3 Executor Pod 事件驱动模型',ST['h2']))
    E.append(Paragraph('Executor Pod管理采用 <b>生产者-消费者模型</b>：事件源(Watch增量+Polling全量)产生Pod状态事件 -> <b>ExecutorPodsSnapshotsStoreImpl</b> 聚合为快照(维护currentSnapshot全局视图) -> 时间窗口批量通知两个消费者。<b>ExecutorPodsAllocator</b> 负责计算missing数并创建/删除Pod；<b>ExecutorPodsLifecycleManager</b> 负责处理Pod终态(Failed/Succeeded/Deleted)并通知Driver移除Executor。',ST['b']))
    E.append(Paragraph('图 2-2: Executor Pod 事件驱动模型',ST['cap']))
    E.append(f3())

    E.append(Paragraph('2.4 Pod 分配器策略',ST['h2']))
    E.append(tb([['策略','配置值','类名','适用场景'],
        ['直接Pod','direct(默认)','ExecutorPodsAllocator','通用场景,直接创建独立Pod'],
        ['StatefulSet','statefulset','StatefulSetPodsAllocator','需要稳定网络标识'],
        ['Deployment','deployment','DeploymentPodsAllocator','需要滚动更新和自愈']],[65,80,155,160],CK8))
    E.append(Paragraph('三者均继承 <b>AbstractPodsAllocator</b>，通过配置选择。在Pod创建和删除方式上不同，但共享相同的快照事件处理机制。',ST['b']))
    E.append(PageBreak())

    E.append(Paragraph('2.5 核心类继承体系',ST['h2']))
    E.append(Paragraph('图 2-3: Kubernetes模块核心类继承体系',ST['cap']))
    E.append(f4())
    E.append(Paragraph('核心类说明：',ST['h3']))
    E.append(tb([['类名','职责','规模'],
        ['KubernetesClusterManager','SPI入口,创建Scheduler和Backend','~60行'],
        ['KubernetesClusterSchedulerBackend','调度后端,桥接Spark调度器和K8s API','~400行'],
        ['KubernetesClientApplication','Cluster Mode提交入口','~260行'],
        ['KubernetesDriverBuilder','Driver Pod构建器,串联Feature Step链','~110行'],
        ['KubernetesExecutorBuilder','Executor Pod构建器','~96行'],
        ['ExecutorPodsAllocator','Pod分配器,核心事件消费者','~630行'],
        ['ExecutorPodsLifecycleManager','Pod生命周期管理','~340行'],
        ['ExecutorPodsSnapshotsStoreImpl','快照存储,生产者-消费者中间件','~170行'],
        ['KubernetesConf','配置抽象基类(Driver/Executor子类)','~336行'],
        ['KubernetesFeatureConfigStep','核心trait,Pod配置步骤接口','~86行'],
        ['BasicDriverFeatureStep','Driver核心Feature(镜像/CPU/内存)','~197行'],
        ['BasicExecutorFeatureStep','Executor核心Feature','~333行']],[175,240,50],CK8))
    E.append(PageBreak())

    E.append(Paragraph('2.6 Shuffle 与 PVC 复用',ST['h2']))
    E.append(Paragraph('K8s环境通过 <b>KubernetesLocalDiskShuffleDataIO</b> 和 <b>KubernetesLocalDiskShuffleExecutorComponents</b> 实现基于PVC的Shuffle数据恢复机制：',ST['b']))
    for t in ['<bullet>&bull;</bullet><b>PVC复用</b>: reusePVC=true时,新Executor优先复用已释放PVC,按storageClass和size匹配',
              '<bullet>&bull;</bullet><b>数据恢复</b>: 扫描本地目录->找data/index/checksum->校验->注册到BlockManager',
              '<bullet>&bull;</bullet><b>等待复用</b>: waitToReusePVC=true且达maxPVCs上限时等待PVC释放']:
        E.append(Paragraph(t,ST['bu']))

    E.append(Paragraph('2.7 插件扩展',ST['h2']))
    E.append(tb([['插件','类名','功能','触发条件'],
        ['滚动更新','ExecutorRollPlugin','周期选择executor decommission替换\n策略:ID/GC_TIME/DURATION/OUTLIER等','rollInterval>0'],
        ['内存扩容','ExecutorResizePlugin','K8s metrics检测+原地扩容\n利用KEP 1287原地Pod更新','resize.enabled=true']],[55,110,175,120],CK8))
    E.append(PageBreak())

    # ═══ 第三章 YARN ═══
    E.append(Paragraph('第三章  YARN 资源管理器',ST['h1']))
    E.append(Paragraph('Spark on YARN模块位于 <font face="Courier">resource-managers/yarn</font>，约25个Scala+4个Java文件、~7000行代码。核心文件：<b>Client.scala</b>(76KB/1836行)、<b>YarnAllocator.scala</b>(49KB/1065行)、<b>ApplicationMaster.scala</b>(39KB/977行)。YARN模块的一大特色是支持 <b>Client Mode</b> 和 <b>Cluster Mode</b> 两种部署模式。',ST['b']))

    E.append(Paragraph('3.1 Client Mode 完整流程',ST['h2']))
    E.append(Paragraph('Client Mode下Driver运行在用户端机器，AM仅作为 <b>ExecutorLauncher</b> 负责资源分配。YarnClientSchedulerBackend.start()中创建Client对象并submitApplication()，然后waitForApplication()轮询至RUNNING，最后启动MonitorThread后台监控。AM启动后通过RPC向Driver发送RegisterClusterManager注册自身。',ST['b']))
    E.append(Paragraph('图 3-1: YARN Client Mode 完整流程',ST['cap']))
    E.append(f5())
    E.append(PageBreak())

    E.append(Paragraph('3.2 Cluster Mode 完整流程',ST['h2']))
    E.append(Paragraph('Cluster Mode下Driver运行在AM容器内。AM的runDriver()方法在独立线程中通过反射调用用户主类，SparkContext初始化时调用sparkContextInitialized()设置Promise。AM等待Promise完成后registerAM()、createAllocator()、最后resumeDriver()唤醒用户线程继续执行。',ST['b']))
    E.append(Paragraph('图 3-2: YARN Cluster Mode 完整流程',ST['cap']))
    E.append(f6())

    E.append(Paragraph('3.2.1 Client vs Cluster 关键区别',ST['h3']))
    E.append(tb([['维度','Client Mode','Cluster Mode'],
        ['Driver位置','客户端机器','AM容器内'],
        ['AM角色','仅资源分配(ExecutorLauncher)','Driver+资源分配'],
        ['AM内存','spark.yarn.am.memory','spark.driver.memory'],
        ['AM核数','spark.yarn.am.cores','spark.driver.cores'],
        ['TaskScheduler','YarnScheduler','YarnClusterScheduler'],
        ['SchedulerBackend','YarnClientSchedulerBackend','YarnClusterSchedulerBackend'],
        ['attemptId','None','Some(attemptId)'],
        ['AM注册端口','-1(不监听)','Driver实际端口']],[90,180,190],CYN))
    E.append(PageBreak())

    E.append(Paragraph('3.3 YarnAllocator 资源分配详解',ST['h2']))
    E.append(Paragraph('<b>YarnAllocator</b> 是YARN资源管理的核心引擎(1065行)，负责容器请求、分配、启动和回收。使用YARN的Priority机制(用ResourceProfile ID作Priority值)区分不同资源需求。核心循环allocateResources()包含4步：updateResourceRequests(计算missing并发送请求)、amClient.allocate(心跳)、handleAllocatedContainers(三级匹配+启动)、processCompletedContainers(退出处理)。',ST['b']))
    E.append(Paragraph('图 3-3: YarnAllocator 资源分配核心流程',ST['cap']))
    E.append(f7())

    E.append(Paragraph('关键数据结构：',ST['h3']))
    E.append(tb([['数据结构','类型','用途'],
        ['allocatedHostToContainersMapPerRPId','Map[Int,Map[String,Set]]','每个RP的host->容器映射'],
        ['executorIdToContainer','Map[String,Container]','executorId->Container映射'],
        ['rpIdToYarnResource','Map[Int,Resource]','RP ID->YARN Resource映射'],
        ['targetNumExecutorsPerRPId','Map[Int,Int]','每个RP的目标executor数'],
        ['failedExecutorsTimeStamps','Map[String,Long]','失败executor时间戳'],
        ['pendingLossReasonRequests','Map[String,...]','待处理的丢失原因请求']],[165,130,165]))
    E.append(PageBreak())

    E.append(Paragraph('3.4 本地性匹配策略',ST['h2']))
    E.append(Paragraph('<b>LocalityPreferredContainerPlacementStrategy</b> 实现了智能容器放置算法，目标是最大化任务本地执行。根据各host的任务数量比例计算每个新容器应偏好的节点和机架。算法分为：计算expectedHostToContainerCount -> 分localityFree和localityAware两组 -> 对localityAware按比例轮询分配偏好。',ST['b']))
    E.append(Paragraph('图 3-4: 本地性偏好容器放置策略',ST['cap']))
    E.append(f9())

    E.append(Paragraph('三级容器匹配流程：',ST['h3']))
    for t in ['<bullet>&bull;</bullet><b>第1级 NODE_LOCAL</b>: 按主机名精确匹配,容器与请求在同一节点',
              '<bullet>&bull;</bullet><b>第2级 RACK_LOCAL</b>: 解析机架信息(独立线程避免阻塞),按机架匹配',
              '<bullet>&bull;</bullet><b>第3级 OFF_RACK (ANY)</b>: 剩余容器匹配任意待处理请求',
              '<bullet>&bull;</bullet><b>释放多余</b>: 无法匹配的容器通过releaseAssignedContainer释放回RM']:
        E.append(Paragraph(t,ST['bu']))
    E.append(PageBreak())

    E.append(Paragraph('3.5 核心类继承体系',ST['h2']))
    E.append(Paragraph('图 3-5: YARN模块核心类继承体系',ST['cap']))
    E.append(f8())
    E.append(Paragraph('核心类说明：',ST['h3']))
    E.append(tb([['类名','职责','规模'],
        ['YarnClusterManager','SPI入口,根据deployMode创建不同组件','~57行'],
        ['YarnSchedulerBackend','公共抽象基类,含YarnSchedulerEndpoint','~412行'],
        ['YarnClientSchedulerBackend','Client模式后端,提交应用+MonitorThread','~190行'],
        ['YarnClusterSchedulerBackend','Cluster模式后端,从AM获取attemptId','~51行'],
        ['Client','YARN提交客户端(最大文件)','~1836行'],
        ['ApplicationMaster','AM主逻辑,runDriver/runExecutorLauncher','~977行'],
        ['YarnAllocator','资源分配器,容器请求/分配/启动/回收','~1065行'],
        ['ExecutorRunnable','NMClient启动Executor容器','~232行'],
        ['YarnRMClient','AMRMClient封装','~154行'],
        ['LocalityPreferredContainerPlacementStrategy','容器放置本地性策略','~231行'],
        ['YarnAllocatorNodeHealthTracker','节点健康追踪与排除管理','~153行'],
        ['ResourceRequestHelper','自定义YARN资源(GPU/FPGA)映射','~190行']],[180,235,50],CYN))
    E.append(PageBreak())

    E.append(Paragraph('3.6 AM 与 RM 交互机制',ST['h2']))
    E.append(Paragraph('ApplicationMaster 通过 <b>YarnRMClient</b> 与YARN RM交互，使用Hadoop的AMRMClient实现心跳、资源请求和注销：',ST['b']))
    E.append(tb([['交互阶段','方法','说明'],
        ['注册','amClient.registerApplicationMaster()','传入host/port/trackingUrl'],
        ['心跳','amClient.allocate(0.1f)','返回AllocateResponse(新分配+已完成+节点变更)'],
        ['请求容器','amClient.addContainerRequest()','带优先级、节点/机架偏好的ContainerRequest'],
        ['取消请求','amClient.removeContainerRequest()','取消pending中的请求'],
        ['释放容器','amClient.releaseAssignedContainer()','释放不需要的容器'],
        ['节点排除','amClient.updateBlacklist()','添加/移除排除节点列表'],
        ['注销','amClient.unregisterApplicationMaster()','传入最终状态和诊断信息']],[65,185,210]))

    E.append(Paragraph('Reporter Thread 心跳机制：',ST['h3']))
    E.append(Paragraph('AM内部的Reporter Thread以自适应间隔循环调用allocateResources()：有pending请求时使用短间隔(指数退避)，无pending时使用heartbeatInterval。每次循环还检查executor失败数是否超限、是否所有节点被排除。',ST['b']))

    E.append(Paragraph('YarnSchedulerEndpoint RPC通信：',ST['h3']))
    E.append(Paragraph('Driver端的 <b>YarnSchedulerEndpoint</b> 是AM与Driver的RPC桥梁。AM启动时发送RegisterClusterManager注册自身引用。之后Driver的RequestExecutors/KillExecutors请求通过此端点转发到AM的AMEndpoint，AMEndpoint再调用YarnAllocator的对应方法。<b>YarnDriverEndpoint</b> 重写了onDisconnected方法，在Executor断连时先向AM询问退出原因(如YARN抢占)，避免误判任务失败。',ST['b']))
    E.append(PageBreak())

    # ═══ 第四章 对比 ═══
    E.append(Paragraph('第四章  Kubernetes vs YARN 架构对比',ST['h1']))
    E.append(Paragraph('图 4-1: 两种资源管理器架构对比',ST['cap']))
    E.append(f10())
    E.append(Spacer(1,10))
    E.append(Paragraph('详细对比分析：',ST['h2']))
    E.append(tb([['对比维度','Kubernetes','YARN'],
        ['资源抽象','Pod(可包含多容器)','Container(单进程)'],
        ['资源申请方式','直接通过K8s API创建Pod','通过AMRMClient心跳时携带请求'],
        ['资源配置模式','Feature Step链式(12步,可扩展)','单一方法createContainerLaunchContext'],
        ['事件监控','Watch(实时增量)+Polling(全量同步)\n生产者-消费者快照模型','AMRMClient.allocate()返回已完成列表\n心跳轮询模型'],
        ['本地性策略','无(K8s自身调度)','三级匹配:HOST->RACK->ANY\nLocalityPreferredPlacement'],
        ['Executor注册','GenerateExecID(K8s为Pod分配ID)\n再RegisterExecutor','直接RegisterExecutor\n(ID由Driver分配)'],
        ['部署模式','仅Cluster Mode','Client Mode + Cluster Mode'],
        ['弹性伸缩','3种Pod分配器\n(Direct/StatefulSet/Deployment)','AMRMClient容器请求增减'],
        ['故障恢复','PVC复用+Shuffle数据恢复','容器重试+AM重试'],
        ['安全模型','K8s RBAC+ServiceAccount','Kerberos+Token续期+AMIPFilter'],
        ['插件扩展','ExecutorRoll/Resize Plugin','无内置插件机制'],
        ['代码规模','~40文件, ~6000行','~29文件, ~7000行']],[70,210,180]))
    E.append(PageBreak())

    # ═══ 第五章 设计精髓 ═══
    E.append(Paragraph('第五章  核心设计精髓总结',ST['h1']))

    E.append(Paragraph('5.1 ExternalClusterManager SPI 可插拔架构',ST['h2']))
    E.append(Paragraph('Spark通过Java SPI机制加载资源管理器实现，核心接口 <b>ExternalClusterManager</b> 只需实现4个方法：canCreate(判断master URL)、createTaskScheduler、createSchedulerBackend、initialize。这使得添加新的资源管理器只需编写一个实现类并注册到META-INF/services即可，完全不需要修改Spark核心代码。',ST['b']))

    E.append(Paragraph('5.2 Feature Step 链式模式 (K8s)',ST['h2']))
    E.append(Paragraph('K8s模块的Feature Step链式模式是一种优雅的<b>管道-过滤器(Pipeline-Filter)</b>设计模式应用。每个Feature只关注自己的配置维度(如Secret、Volume、网络策略)，通过foldLeft串联实现关注点分离。用户可以通过配置注入自定义Step，实现零代码修改的Pod定制。这种设计既保证了内置功能的完整性，又提供了极强的扩展性。',ST['b']))

    E.append(Paragraph('5.3 生产者-消费者事件驱动 (K8s)',ST['h2']))
    E.append(Paragraph('K8s模块的Pod管理采用了经典的生产者-消费者模型，通过 <b>ExecutorPodsSnapshotsStoreImpl</b> 解耦事件源和处理器。Watch提供实时增量更新，Polling提供定期全量同步(防止Watch事件丢失)，两者互补保证一致性。时间窗口批量通知机制减少了不必要的处理频率，提高了系统稳定性。',ST['b']))

    E.append(Paragraph('5.4 AM双模式架构 (YARN)',ST['h2']))
    E.append(Paragraph('YARN模块通过同一个 <b>ApplicationMaster</b> 类支持两种截然不同的运行模式：Cluster Mode下AM既是Driver又是资源管理器；Client Mode下AM仅作为ExecutorLauncher。通过 <font face="Courier">isClusterMode = (args.userClass != null)</font> 一行代码区分，然后分别进入runDriver()和runExecutorLauncher()两条执行路径。这种设计避免了代码重复，最大化了复用。',ST['b']))

    E.append(Paragraph('5.5 三级本地性匹配 (YARN)',ST['h2']))
    E.append(Paragraph('YARN模块的容器分配实现了精细的本地性优化：首先通过 <b>LocalityPreferredContainerPlacementStrategy</b> 按任务分布比例计算容器偏好，然后在handleAllocatedContainers()中按 HOST->RACK->ANY 三级匹配。这种两阶段本地性策略既充分利用了数据本地性，又避免了资源浪费。',ST['b']))

    E.append(Paragraph('5.6 CoarseGrainedSchedulerBackend 统一基类',ST['h2']))
    E.append(Paragraph('K8s和YARN的调度后端都继承自 <b>CoarseGrainedSchedulerBackend</b>，共享Executor注册、任务分发、资源请求等核心逻辑。各资源管理器只需关注自己平台特有的资源申请和生命周期管理，极大降低了开发和维护成本。DriverEndpoint RPC通信框架为两种部署模式提供了统一的Executor管理接口。',ST['b']))

    E.append(Spacer(1,20))
    E.append(Paragraph('— 文档结束 —',ParagraphStyle('End',fontName=FN,fontSize=12,alignment=TA_CENTER,textColor=HexColor('#9ca3af'))))

    doc.build(E)
    print(f'PDF generated: {out}')
    return out

if __name__ == '__main__':
    build()
