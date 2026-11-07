"""Build the 16-slide Tetun project presentation using only Python stdlib."""
from __future__ import annotations

from html import escape
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs" / "apresentasaun_analitika_ecommerce_tetun.pptx"
EMU = 914400
SW, SH = 13.333333, 7.5

NAVY = "080D21"
PANEL = "11182E"
PANEL2 = "171F3A"
WHITE = "F4F6FF"
MUTED = "A8B3CB"
BLUE = "458BFF"
CYAN = "36D5D0"
PURPLE = "A77BFF"
GOLD = "FFC46B"
CORAL = "FF8F87"
GREEN = "43D6AE"
FONT = "Aptos"


def emu(value: float) -> int:
    return int(value * EMU)


def xesc(value: object) -> str:
    return escape(str(value), quote=False)


class Slide:
    def __init__(self, number: int, section: str, title: str, subtitle: str = ""):
        self.number = number
        self.shapes: list[str] = []
        self.next_id = 2
        self.rect(0, 0, SW, SH, NAVY, line=NAVY)
        self.rect(0, 0, .12, SH, BLUE, line=BLUE)
        self.rect(.55, .42, .08, .27, CYAN, line=CYAN)
        self.text(.75, .35, 11.8, .28, section.upper(), 9, CYAN, bold=True, char_space=1.2)
        self.text(.55, .77, 12.1, .56, title, 25, WHITE, bold=True)
        if subtitle:
            self.text(.58, 1.35, 12.0, .43, subtitle, 11, MUTED)
        self.rect(.55, 7.08, 12.23, .012, "28314D", line="28314D")
        self.text(.58, 7.13, 10.7, .19,
                  "Monitorizasaun dadus Retailrocket ho Kafka no Spark", 8, MUTED)
        self.text(11.85, 7.12, .9, .2, f"{number:02d} / 16", 8, CYAN,
                  bold=True, align="r")

    def _base(self, name: str, x: float, y: float, w: float, h: float,
              geom: str, fill: str | None, line: str | None, line_width: float = 1):
        sid = self.next_id
        self.next_id += 1
        fill_xml = '<a:noFill/>' if fill is None else f'<a:solidFill><a:srgbClr val="{fill}"/></a:solidFill>'
        line_xml = '<a:ln><a:noFill/></a:ln>' if line is None else (
            f'<a:ln w="{int(line_width*12700)}"><a:solidFill><a:srgbClr val="{line}"/></a:solidFill></a:ln>')
        return sid, (f'<p:sp><p:nvSpPr><p:cNvPr id="{sid}" name="{escape(name)}"/>'
            f'<p:cNvSpPr/><p:nvPr/></p:nvSpPr><p:spPr><a:xfrm><a:off x="{emu(x)}" y="{emu(y)}"/>'
            f'<a:ext cx="{emu(w)}" cy="{emu(h)}"/></a:xfrm><a:prstGeom prst="{geom}"><a:avLst/></a:prstGeom>'
            f'{fill_xml}{line_xml}</p:spPr>')

    def rect(self, x: float, y: float, w: float, h: float, fill: str,
             line: str | None = None, radius: bool = False, line_width: float = 1):
        _, xml = self._base("rectangle", x, y, w, h,
                            "roundRect" if radius else "rect", fill, line or fill, line_width)
        self.shapes.append(xml + '</p:sp>')

    def text(self, x: float, y: float, w: float, h: float, value: str,
             size: float = 14, color: str = WHITE, bold: bool = False,
             align: str = "l", valign: str = "t", margin: float = .04,
             char_space: float = 0, italic: bool = False):
        sid, xml = self._base("text", x, y, w, h, "rect", None, None)
        algn = {"l": "l", "c": "ctr", "r": "r"}.get(align, "l")
        anchor = {"t": "t", "m": "ctr", "b": "b"}.get(valign, "t")
        paragraphs = []
        for line in value.split("\n"):
            paragraphs.append(
                f'<a:p><a:pPr algn="{algn}"/><a:r><a:rPr lang="tet" sz="{int(size*100)}" '
                f'b="{1 if bold else 0}" i="{1 if italic else 0}" spc="{int(char_space*100)}">'
                f'<a:solidFill><a:srgbClr val="{color}"/></a:solidFill><a:latin typeface="{FONT}"/>'
                f'</a:rPr><a:t xml:space="preserve">{xesc(line)}</a:t></a:r>'
                f'<a:endParaRPr lang="tet" sz="{int(size*100)}"/></a:p>')
        body = (f'<p:txBody><a:bodyPr wrap="square" lIns="{emu(margin)}" rIns="{emu(margin)}" '
                f'tIns="{emu(margin)}" bIns="{emu(margin)}" anchor="{anchor}"><a:noAutofit/></a:bodyPr>'
                f'<a:lstStyle/>{"".join(paragraphs)}</p:txBody>')
        self.shapes.append(xml + body + '</p:sp>')

    def line(self, x1: float, y1: float, x2: float, y2: float,
             color: str = BLUE, width: float = 1.5, arrow: bool = False):
        sid = self.next_id
        self.next_id += 1
        dx, dy = x2 - x1, y2 - y1
        x, y = min(x1, x2), min(y1, y2)
        flip_h = ' flipH="1"' if dx < 0 else ''
        flip_v = ' flipV="1"' if dy < 0 else ''
        head = '<a:headEnd type="triangle" w="med" len="med"/>' if arrow else ''
        self.shapes.append(
            f'<p:sp><p:nvSpPr><p:cNvPr id="{sid}" name="line"/><p:cNvSpPr/><p:nvPr/></p:nvSpPr>'
            f'<p:spPr><a:xfrm{flip_h}{flip_v}><a:off x="{emu(x)}" y="{emu(y)}"/>'
            f'<a:ext cx="{max(1,emu(abs(dx)))}" cy="{max(1,emu(abs(dy)))}"/></a:xfrm>'
            f'<a:prstGeom prst="line"><a:avLst/></a:prstGeom><a:ln w="{int(width*12700)}">'
            f'<a:solidFill><a:srgbClr val="{color}"/></a:solidFill>{head}</a:ln></p:spPr></p:sp>')

    def card(self, x: float, y: float, w: float, h: float, title: str,
             body: str = "", accent: str = BLUE, title_size: float = 14,
             body_size: float = 11, fill: str = PANEL):
        self.rect(x, y, w, h, fill, line=accent, radius=True)
        self.rect(x + .13, y + .16, .06, .26, accent, line=accent)
        self.text(x + .27, y + .1, w - .4, .4, title, title_size, WHITE, bold=True)
        if body:
            self.text(x + .2, y + .57, w - .4, h - .68, body, body_size, MUTED)

    def bullet_list(self, x: float, y: float, w: float, h: float,
                    items: list[str], size: float = 15, color: str = WHITE,
                    gap: float = .52, bullet_color: str = CYAN):
        for i, item in enumerate(items):
            yy = y + i * gap
            self.rect(x, yy + .12, .09, .09, bullet_color, line=bullet_color, radius=True)
            self.text(x + .2, yy, w - .2, gap - .02, item, size, color)

    def bar(self, x: float, y: float, max_w: float, label: str,
            value: int, maximum: int, color: str, suffix: str = ""):
        self.text(x, y, 2.2, .3, label, 10, MUTED)
        self.rect(x + 2.22, y + .04, max_w - 3.25, .18, "202944", line="202944", radius=True)
        width = max(.06, (max_w - 3.25) * (value / maximum if maximum else 0))
        self.rect(x + 2.22, y + .04, width, .18, color, line=color, radius=True)
        self.text(x + max_w - 1.0, y - .02, 1.0, .28,
                  f"{value:,}{suffix}", 9, WHITE, bold=True, align="r")

    def xml(self) -> str:
        tree = ('<p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/>'
                '<p:nvPr/></p:nvGrpSpPr><p:grpSpPr><a:xfrm><a:off x="0" y="0"/>'
                '<a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/>'
                '</a:xfrm></p:grpSpPr>')
        tree += ''.join(self.shapes) + '</p:spTree>'
        return (f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<p:sld xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
            f'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
            f'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">'
            f'<p:cSld name="{self.number:02d}">{tree}</p:cSld><p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr></p:sld>')


def slide_content() -> list[Slide]:
    slides: list[Slide] = []

    # 1 — Title and team: clear, high-contrast opening slide.
    s = Slide(1, "Títulu no ekipa", "Analítika ba Komérsiu Elektróniku iha Tempo Real", "")
    s.rect(.72, 1.62, 11.9, 5.12, PANEL, line="354568", radius=True)
    s.rect(.72, 1.62, .13, 5.12, CYAN, line=CYAN)
    s.text(1.15, 1.98, 7.2, .35, "APRESENTASAUN PROJETU", 10, CYAN, bold=True, char_space=1.6)
    s.text(1.15, 2.52, 7.15, 1.18, "Monitoriza Click, View\nno Sesaun la iha Sosa", 27, WHITE, bold=True)
    s.text(1.18, 3.92, 6.65, .65,
           "Uza Apache Kafka no Apache Spark atu analiza eventu istóriku Retailrocket.", 15, MUTED)
    s.text(1.18, 4.82, 6.55, .63,
           "Replay istóriku / simulasaun iha tempu real; la'ós tráfiku live hosi loja.", 11, GOLD, bold=True)
    s.text(1.18, 5.82, 6.4, .38, "Apresenta husi: Epifania", 12, CYAN, bold=True)
    s.text(1.18, 6.24, 6.55, .25, "2026  ·  Retailrocket  ·  Big Data Analytics", 9, MUTED)
    # Decorative pipeline motif at right, based on the actual system components.
    s.rect(8.25, 2.28, 3.8, 3.62, "0B1124", line="293454", radius=True)
    s.text(8.55, 2.58, 3.2, .32, "FLUXU PIPELINE", 10, MUTED, bold=True, char_space=1.0)
    nodes = [("CSV", CYAN), ("Kafka", PURPLE), ("Spark", GOLD), ("Dashboard", GREEN)]
    for i, (label, color) in enumerate(nodes):
        yy = 3.12 + i*.58
        s.rect(8.58, yy, .34, .34, color, line=color, radius=True)
        s.text(9.1, yy-.015, 2.25, .34, label, 12, WHITE, bold=True, valign="m")
        if i < len(nodes)-1: s.line(8.75, yy+.37, 8.75, yy+.54, "536384", 1.4, True)
    s.text(1.18, 6.68, 11.1, .16,
           "Títulu ofisiál: E-Commerce Real-Time Analytics: Monitoring Click, View, and No Purchase Using Apache Kafka and Apache Spark", 6.2, MUTED)
    slides.append(s)

    # 2 — Background: why product analytics matters to a company.
    s = Slide(2, "Kontekstu", "Tanbasá empresa analiza produtu sira?", "Analiza transforma eventu sira sai informasaun atu ajuda desizaun negósiu")
    s.card(.75, 2.05, 3.75, 2.0, "View", "Konta hira ema haree produtu ida. Ida ne'e hatudu atensaun, maibé la signifika katak ema sosa.", CYAN, 18, 13)
    s.card(4.8, 2.05, 3.75, 2.0, "Add to cart", "Hatudu interese atu sosa. Kompara ho view atu haree iha ne'ebé kliente para iha prosesu sosa.", PURPLE, 18, 13)
    s.card(8.85, 2.05, 3.75, 2.0, "Transaction", "Hatudu sosa ne'ebé rejistada. Kompara ho eventu seluk atu komprende konversaun.", GOLD, 18, 13)
    s.rect(.75, 4.45, 11.85, 1.45, "10172D", line="293454", radius=True)
    s.text(1.05, 4.7, 11.3, .38, "Utilidade ba empresa", 15, CYAN, bold=True)
    s.text(1.05, 5.14, 11.2, .55,
           "Rezultadu ajuda planeia stock, promosaun no hadia pajina produtu. Analiza hatudu padraun; la prova razaun kliente ida-idak.", 13, WHITE)
    slides.append(s)

    # 3 — Problem: a business problem, not a dataset description.
    s = Slide(3, "Problema", "Problema negósiu: performansa produtu la klaru", "")
    s.rect(.8, 1.85, 11.75, 4.55, PANEL, line="354568", radius=True)
    s.rect(.8, 1.85, .12, 4.55, CORAL, line=CORAL)
    s.text(1.2, 2.18, 1.45, .78, "03", 39, CORAL, bold=True)
    s.text(1.2, 3.05, 1.45, .34, "PROBLEMA", 9, MUTED, bold=True, char_space=1.1)
    s.rect(2.85, 2.25, .025, 3.55, "354568", line="354568")
    problem_paragraph = ("Empresa bele rekolla eventu view, add to cart no transaction, maibé se la organiza no analiza eventu sira, susar atu hatene produtu ne'ebé atrai atensaun, iha ne'ebé kliente para, no parte ida ne'ebé presiza hadia. Nune'e, desizaun kona-ba stock, promosaun no pajina produtu bele bazeia de'it ba intuisaun, la'ós evidénsia ne'ebé bele kompara. Projetu ida ne'e responde ba nesesidade ne'e ho pipeline ne'ebé prosesu eventu no hatudu metrika produtu no prosesu ne'ebé bele haree no verifica.")
    s.text(3.25, 2.42, 8.65, 3.25, problem_paragraph, 19, WHITE, valign="m", margin=.16)
    s.rect(3.25, 5.77, 1.35, .045, CORAL, line=CORAL)
    slides.append(s)

    # 4 — Objectives: the purpose and delivered system capabilities.
    s = Slide(4, "Objetivu sira", "Objetivu prinsipál projetu", "")
    s.rect(.8, 1.85, 11.75, 4.55, PANEL, line="354568", radius=True)
    s.rect(.8, 1.85, .12, 4.55, CYAN, line=CYAN)
    s.text(1.2, 2.18, 1.45, .78, "04", 39, CYAN, bold=True)
    s.text(1.2, 3.05, 1.45, .34, "OBJETIVU", 9, MUTED, bold=True, char_space=1.1)
    s.rect(2.85, 2.25, .025, 3.55, "354568", line="354568")
    objective_paragraph = ("Objetivu projetu ne'e mak harii sistema analítiku atu monitoriza performansa produtu no alur kliente. Producer replay eventu istóriku ho taxa ne'ebé bele kontrola ba Apache Kafka; Apache Spark valida, deduplika no agrega view, add to cart, transaction no sesaun ne'ebé la iha transasaun rejistada. Sistema rai rezultadu iha database no hatudu iha dashboard Streamlit, atu utilizadór bele kompara tendénsia produtu, purchase funnel no evidénsia pipeline, no uza informasaun ne'e atu foti desizaun ne'ebé bazeia ba dadus.")
    s.text(3.25, 2.42, 8.65, 3.25, objective_paragraph, 18, WHITE, valign="m", margin=.16)
    s.rect(3.25, 5.77, 1.35, .045, CYAN, line=CYAN)
    slides.append(s)

    # 5 — Data source
    s = Slide(5, "Fonte dadus", "Fonte: Retailrocket E-commerce Dataset", "Dadus orijinál iha pasta `data/` sei nafatin la muda")
    s.card(.8, 2.0, 5.65, 3.7, "Arquivu prinsipál",
           "events.csv\n\nIha timestamp, visitorid, event, itemid, no transactionid bainhira iha.\n\nFonte rejista eventu iha tinan 2015.", CYAN, 17, 13)
    s.card(6.7, 2.0, 5.65, 3.7, "Arquivu suporta",
           "category_tree.csv — ligasaun jerárkika ba kategoria.\n\nitem_properties_part1.csv no part2.csv — propriedade item, maibé la fó naran produtu verifikadu.", PURPLE, 16, 13)
    s.text(.95, 6.05, 11.3, .48,
           "Dadus ne'e uza ba Historical Event Replay / Real-Time Simulation; la simu tráfiku live hosi loja.", 12, GOLD, bold=True)
    slides.append(s)

    # 6 — Methodology
    s = Slide(6, "Metodolojia", "Dalan hosi CSV to'o informasaun analítika", "Prosesu bele repete no akompanha iha etapa ida-idak")
    steps = [
        ("01", "Auditoria", "Kontajen rejistu, tipu eventu, no valor ne'ebé falta."),
        ("02", "Injesaun", "Replay producer lee eventu no haruka ba Kafka."),
        ("03", "Prosesamentu", "Spark valida timestamp no schema, deduplika, no agrega."),
        ("04", "Armazenamentu", "JSON prosesadu no tabela SQLite rai rezultadu."),
        ("05", "Vizualizasaun", "Streamlit hatudu métrika, eventu, no estadu pipeline."),
    ]
    for i, (n, title, body) in enumerate(steps):
        x = .55 + i*2.55
        s.card(x, 2.45, 2.25, 2.45, f"{n} · {title}", body,
               [CYAN, BLUE, PURPLE, GOLD, GREEN][i], 13, 11)
        if i < 4: s.line(x+2.27, 3.65, x+2.5, 3.65, CYAN, 1.8, True)
    s.text(.85, 5.45, 11.6, .5,
           "Eventu nia oras orijinál, oras simu, no oras prosesamentu rejista separadamente.", 14, WHITE, bold=True, align="c")
    slides.append(s)

    # 7 — Architecture
    s = Slide(7, "Arkitetura sistema", "Pipeline Big Data ne'ebé implementa", "Kafka transporta eventu; Spark Structured Streaming halo prosesamentu real")
    nodes = [
        ("Retailrocket", "CSV istóriku", CYAN),
        ("Replay producer", "Taxa konfigurável", BLUE),
        ("Apache Kafka", "retailrocket-events", PURPLE),
        ("Apache Spark", "Validasaun + agregasaun", GOLD),
        ("SQLite + JSON", "Rezultadu persistente", GREEN),
        ("Streamlit", "Dashboard web", CORAL),
    ]
    for i, (name, desc, color) in enumerate(nodes):
        x = .48 + i*2.13
        s.card(x, 2.65, 1.83, 1.75, name, desc, color, 12, 10)
        if i < len(nodes)-1: s.line(x+1.85, 3.5, x+2.1, 3.5, CYAN, 1.6, True)
    s.rect(.8, 5.0, 5.55, .92, "131B35", line="263452", radius=True)
    s.text(1.05, 5.2, 5.0, .5, "Kafka no Spark: Docker Compose", 13, WHITE, bold=True)
    s.rect(6.85, 5.0, 5.55, .92, "131B35", line="263452", radius=True)
    s.text(7.1, 5.2, 5.0, .5, "Dashboard: Python + Streamlit iha host", 13, WHITE, bold=True)
    slides.append(s)

    # 8 — Dataset, with genuine examples read from the source CSV.
    s = Slide(8, "Konjuntu dadus", "Dadus orijinál no ezemplu rejistu", "Kontajen hosi data/events.csv; eventu ida la'ós vizitante ida")
    stats = [("Rejistu hotu", "2,756,101", BLUE), ("View", "2,664,312", CYAN),
             ("Add to cart", "69,332", PURPLE), ("Transaction", "22,457", GOLD)]
    for i, (label, value, color) in enumerate(stats):
        x = .7 + i*3.12
        s.card(x, 1.95, 2.75, 1.24, label, value, color, 12, 18)

    # Compact source-event distribution on the left.
    s.rect(.72, 3.5, 5.7, 2.95, PANEL, line="293454", radius=True)
    s.text(.98, 3.72, 5.1, .31, "Kompozisaun eventu iha fonte", 13, WHITE, bold=True)
    for i, (label, value, color) in enumerate([
        ("View", 2_664_312, CYAN), ("Add to cart", 69_332, PURPLE), ("Transaction", 22_457, GOLD)
    ]):
        yy=4.28+i*.62
        s.text(.98, yy, 1.25, .26, label, 9, MUTED)
        s.rect(2.28, yy+.035, 2.65, .17, "202944", line="202944", radius=True)
        s.rect(2.28, yy+.035, max(.06,2.65*value/2_664_312), .17, color, line=color, radius=True)
        s.text(5.03, yy-.015, 1.05, .25, f"{value:,}", 8, WHITE, bold=True, align="r")
    s.text(.98, 6.12, 5.08, .2, "View 96.67%  ·  Kareta 2.52%  ·  Sosa 0.81%", 8, MUTED)

    # Three authentic raw CSV examples; blank transaction IDs are shown as a dash.
    s.rect(6.68, 3.5, 5.92, 2.95, PANEL, line="293454", radius=True)
    s.text(6.94, 3.72, 5.3, .31, "Ezemplu husi rejistu orijinál", 13, WHITE, bold=True)
    cols=[("timestamp",6.94,1.32),("visitorid",8.3,.92),("event",9.28,1.0),("itemid",10.34,.86),("transactionid",11.25,1.08)]
    for label,x,w in cols:
        s.text(x,4.2,w,.32,label,7,CYAN,bold=True)
    rows=[
        ("1433221332117","257597","view","355908","—"),
        ("1433223236124","287857","addtocart","5206","—"),
        ("1433222276276","599528","transaction","356475","4000"),
    ]
    for ri,row in enumerate(rows):
        yy=4.63+ri*.46
        s.rect(6.9,yy-.055,5.45,.38,"151E36" if ri%2==0 else "10172D",line=None,radius=True)
        for ci,(value,(_,x,w)) in enumerate(zip(row,cols)):
            s.text(x,yy,w,.25,value,7.2,WHITE if ci else MUTED,bold=(ci==2))
    s.text(6.94,6.12,5.35,.23,"Ezemplu tolu ne'e lee diretamente hosi CSV orijinál.",8,MUTED)
    slides.append(s)

    # 9 — Preprocessing: explain each implemented transformation.
    s = Slide(9, "Pre-prosesamentu", "Oinsá pipeline prepara eventu sira", "Hosi linha CSV raw to'o output Spark: etapa ida-idak aplika regra ne'ebé bele verifika")
    steps = [
        ("01", "Lee no ordena", "Producer lee timestamp, visitorid, event, itemid no transactionid hosi CSV. Hatudu eventu tuir timestamp molok kria sesaun.", CYAN),
        ("02", "Normaliza", "Timestamp millisecond muda ba oras UTC. Kria event_id, event_time, visitor_id, item_id no kampu sira seluk iha schema komún.", BLUE),
        ("03", "Kria sesaun", "Kada visitor hetan sesaun bazeia ba orden eventu. Inatividade liu 30 minutu hahú sesaun foun; transactionid mamuk kontinua nós ba view/cart.", PURPLE),
        ("04", "Spark valida", "Spark lee JSON hosi Kafka, verifica ID, timestamp, visitor, session, item no event_type; hasai invalid, deduplika tuir event_id, agrega no rai output.", GOLD),
    ]
    for i,(num,title,body,color) in enumerate(steps):
        x=.62+i*3.18
        s.rect(x,2.05,2.92,2.72,PANEL,line=color,radius=True)
        s.text(x+.22,2.25,.65,.45,num,23,color,bold=True)
        s.text(x+.92,2.27,1.75,.38,title,13,WHITE,bold=True)
        s.rect(x+.22,2.88,2.45,.025,"354568",line="354568")
        s.text(x+.22,3.08,2.45,1.4,body,10.2,MUTED)
        if i<3: s.line(x+2.94,3.38,x+3.15,3.38,CYAN,1.6,True)
    s.rect(.78,5.18,11.75,1.02,"10172D",line="293454",radius=True)
    s.text(1.05,5.35,2.3,.28,"Lote Spark ikusikus",12,CYAN,bold=True)
    s.text(3.15,5.32,8.95,.35,"49 simu  →  49 validu  →  0 invalidu  →  0 duplikadu  ·  22.219 segundu",12,WHITE,bold=True)
    s.text(1.05,5.78,10.9,.23,"Regra deduplikasaun: event_id. Regra sesaun: visitor ida-idak, gap inatividade liu 30 minutu.",9,MUTED)
    slides.append(s)

    # 10 — EDA findings
    s = Slide(10, "Analiza esploratória ba dadus", "Rezultadu prinsipal hosi audit EDA", "View domina fonte; partisipasaun eventu ida-idak tenke interpreta ho unidade loos")
    s.rect(.8, 2.0, 7.25, 3.95, PANEL, line="293454", radius=True)
    s.text(1.1, 2.25, 6.6, .4, "Tipu eventu iha dataset tomak", 15, WHITE, bold=True)
    eda = [("View", 2_664_312, "96.67%", CYAN),
           ("Add to cart", 69_332, "2.52%", PURPLE),
           ("Transaction", 22_457, "0.81%", GOLD)]
    for i, (name, value, pct, color) in enumerate(eda):
        y = 3.05 + i*.84
        s.text(1.1, y, 1.5, .27, name, 11, MUTED)
        s.rect(2.6, y+.04, 4.0, .2, "202944", line="202944", radius=True)
        s.rect(2.6, y+.04, max(.08, 4.0*value/2_756_101), .2, color, line=color, radius=True)
        s.text(6.75, y-.02, .95, .3, pct, 11, WHITE, bold=True, align="r")
    s.card(8.45, 2.0, 4.0, 1.7, "Interpretasaun", "Eventu view mak 96.67% hosi rejistu hotu; ida ne'e la'ós persentajen ema ne'ebé sosa.", CYAN, 15, 12)
    s.card(8.45, 4.0, 4.0, 1.95, "Limite interpretasaun", "Dataset la iha click separadu. Naran produtu la iha. Eventu repetidu bele mai hosi vizitante ida.", CORAL, 14, 12)
    slides.append(s)

    # 11 — Data mining model
    s = Slide(11, "Modelu analítiku", "Modelu deskriptivu, la'ós modelu previzaun", "Projetu ida ne'e uza estatístika no regra sesaun; la treina modelu Machine Learning")
    s.card(.85, 2.0, 3.7, 2.65, "Unidade analiza", "Eventu ida\nProdutu item_id ida\nVizitante ida\nSesaun ida", CYAN, 15, 14)
    s.card(4.8, 2.0, 3.7, 2.65, "Regra elegibilidade", "Sesaun tenke iha view ida ka add to cart ida. Sesaun parte depois gap liu 30 minutu.", PURPLE, 15, 13)
    s.card(8.75, 2.0, 3.7, 2.65, "Regra no-purchase", "Sesaun elegível la iha eventu transaction rejistadu iha sesaun ne'e.", GOLD, 15, 13)
    s.rect(1.05, 5.15, 11.25, .95, "17162D", line="594369", radius=True)
    s.text(1.3, 5.36, 10.75, .48,
           "Interpretasaun: la prova katak vizitante nunka sosa ka sosa iha fatin seluk.", 13, CORAL, bold=True, align="c")
    slides.append(s)

    # 12 — Evaluation
    s = Slide(12, "Avaliasaun", "Evidénsia kona-ba pipeline no rezultadu", "Kontajen sira mai hosi audit dataset, Kafka metadata, Spark metrics no replay trace")
    cards = [("Fonte", "2,756,101", "rejistu istóriku", BLUE),
             ("Replay foun", "50", "eventu husu", CYAN),
             ("Kafka ACK", "50", "eventu simu", PURPLE),
             ("Spark unik", "12,516", "eventu kumulativu", GREEN)]
    for i, (title, value, caption, color) in enumerate(cards):
        x = .72 + i*3.1
        s.rect(x, 2.0, 2.72, 1.45, PANEL, line=color, radius=True)
        s.text(x+.18, 2.18, 2.36, .28, title, 11, MUTED, bold=True)
        s.text(x+.18, 2.5, 2.36, .52, value, 24, color, bold=True)
        s.text(x+.18, 3.06, 2.36, .25, caption, 9, MUTED)
    s.card(.85, 3.95, 5.55, 1.75, "Lote Spark ikusikus", "49 eventu simu; 49 validu; invalid 0; duplicate 0.\nDurasaun rejistada: 22.219 segundu.", CYAN, 15, 12)
    s.card(6.75, 3.95, 5.55, 1.75, "Rekonsiliasaun", "Kafka offset range: 12,517; Spark eventu ID uniku: 12,516; metrics rejista duplicate ida.", PURPLE, 15, 12)
    s.text(.95, 6.0, 11.2, .42,
           "Kafka retained, produtor ACK no output Spark iha unidade diferente; la'ós kontajen ne'ebé tenke hanesan hotu.", 10, GOLD, align="c")
    slides.append(s)

    # 13 — Dashboard
    s = Slide(13, "Painel monitorizasaun", "Dashboard web atu haree análize no prosesu", "Streamlit hatudu output dadus ne'ebé prosesadu, la'ós númeru estátiku atu halo hanesan live")
    # Dashboard frame
    s.rect(.72, 1.95, 11.92, 4.8, "0B1124", line="303A5B", radius=True)
    s.rect(.72, 1.95, 11.92, .43, "171E38", line="171E38", radius=True)
    s.text(.95, 2.04, 7.2, .25, "MONITORIZASAUN CLICK · VIEW · NO-PURCHASE", 10, CYAN, bold=True)
    dash = [("Eventu prosesadu", "12,516", BLUE), ("View", "12,111", CYAN),
            ("Add to cart", "298", PURPLE), ("Transaction", "107", GOLD)]
    for i,(label,value,color) in enumerate(dash):
        x=.95+i*2.86
        s.rect(x,2.62,2.55,.78,PANEL,line="2B3553",radius=True)
        s.text(x+.13,2.72,2.25,.2,label,8,MUTED)
        s.text(x+.13,2.98,2.25,.31,value,17,color,bold=True)
    # mini event bars
    s.rect(.95,3.68,6.18,2.55,PANEL,line="2B3553",radius=True)
    s.text(1.18,3.82,5.5,.27,"Kontajen eventu prosesadu",11,WHITE,bold=True)
    # Bar heights are scaled from the actual processed event counts shown above.
    processed_counts=[("View",12111,CYAN),("Add to cart",298,PURPLE),("Transaction",107,GOLD)]
    scale=1.72/12111
    for i,(label,count,color) in enumerate(processed_counts):
        bx=1.48+i*1.72
        height=count*scale
        s.rect(bx,5.94-height,.72,height,color,line=None,radius=True)
        s.text(bx-.22,5.99,.98,.2,label,7,MUTED,align="c")
    s.line(1.2,5.95,6.85,5.95,"35405D",.8)
    s.rect(7.4,3.68,4.52,2.55,PANEL,line="2B3553",radius=True)
    s.text(7.65,3.82,4.0,.27,"Vizualizasaun disponivel",11,WHITE,bold=True)
    for i,(lab,val,col) in enumerate([("Produtu", "item_id", PURPLE),("Sesaun", "no-purchase", GOLD),
                                        ("Pipeline", "Kafka → Spark", CYAN)]):
        yy=4.35+i*.55
        s.rect(7.68,yy,.12,.12,col,line=col,radius=True)
        s.text(7.92,yy-.06,1.55,.25,lab,9,MUTED)
        s.text(9.45,yy-.06,2.1,.25,val,9,WHITE,bold=True)
    s.text(1.0,6.43,11.2,.23,
           "Mockup ilustrativu ho valor real hosi output Spark; dashboard atual uza gráfiku interativu.", 9, MUTED, italic=True)
    slides.append(s)

    # 14 — Live demo / screenshots
    s = Slide(14, "Demonstrasaun no evidénsia", "Replays eventu no akompañamentu ba prosesamentu", "Simulasaun ida hatudu eventu real hosi CSV, Kafka ACK, no rejistu ne'ebé Spark rai")
    s.card(.82, 2.0, 3.45, 1.45, "1 · Fonte istórika", "event_id: retailrocket:11810\ntipu: view · item_id: 283405", CYAN, 14, 11)
    s.card(4.92, 2.0, 3.45, 1.45, "2 · Kafka simu", "Topic: retailrocket-events\npartition: 0 · offset: 12467", PURPLE, 14, 11)
    s.card(9.02, 2.0, 3.45, 1.45, "3 · Spark prosesu", "Spark Structured Streaming\noutput no timestamp rai", GOLD, 14, 11)
    s.line(4.3,2.72,4.87,2.72,CYAN,2,True); s.line(8.4,2.72,8.97,2.72,CYAN,2,True)
    s.rect(.9,4.0,11.5,1.55,"10172D",line="293454",radius=True)
    s.text(1.2,4.2,10.8,.32,"Oras ne'ebé tenke haree ketak",14,WHITE,bold=True)
    s.text(1.2,4.68,10.8,.55,
           "Oras eventu: 2015-06-01 18:52:09 · oras ingestion: 2026-10-22 15:08:58 · oras prosesamentu Spark: 2026-10-22 15:09:13", 11, MUTED)
    s.text(1.0,5.95,11.3,.45,
           "Imajen dashboard no payload ne'e uza ba demonstrasaun; eventu ne'e mai hosi replay Retailrocket, la'ós tráfiku live.", 11, GOLD, align="c")
    slides.append(s)

    # 15 — Conclusion & recommendations
    s = Slide(15, "Konkluzaun no rekomendasaun", "Pipeline hatudu prosesamentu Big Data hosi ponta ba ponta", "Rezultadu tenke hatudu ho evidénsia no limitasaun fonte")
    s.card(.85,2.0,5.55,1.55,"Konkluzaun", "Kafka simu replay; Spark valida no agrega; dashboard no SQLite hatudu rezultadu prosesadu.", GREEN,15,12)
    s.card(6.8,2.0,5.55,1.55,"Limitasaun", "La iha click eventu separadu, naran produtu, ka tráfiku loja live. No-purchase limita ba sesaun rejistada.", CORAL,15,12)
    s.text(.95,4.0,11.4,.35,"Rekomendasaun ba etapa tuirmai",16,WHITE,bold=True)
    s.bullet_list(1.05,4.55,11.3,1.65,[
        "Aumenta rejistu no alerta ba erro Kafka/Spark no tempu batch.",
        "Integra fonte web ne'ebé bele fó click genuínu, depois de sistema atual validu.",
        "Koko amostra replay barak liu no dokumenta offset, batch, no qualidade dadus.",
    ],13,WHITE,.52,CYAN)
    slides.append(s)

    # 16 — Links
    s = Slide(16, "Ligasaun no referénsia", "Video demonstrasaun no repositóriu projetu", "Link públiku sei hatama bainhira video no repositóriu disponivel")
    s.card(.85,2.05,5.55,1.85,"YouTube", "Link ba video: seidauk publika", CORAL,17,14)
    s.card(6.8,2.05,5.55,1.85,"GitHub", "Link ba repositóriu públiku: seidauk disponivel", PURPLE,17,14)
    s.rect(.9,4.45,11.45,1.42,"11182E",line="293454",radius=True)
    s.text(1.2,4.7,10.8,.32,"Referénsia ba ortografia Tetun",14,WHITE,bold=True)
    s.text(1.2,5.12,10.8,.45,
           "Dekretu Governu Repúblika Demokrátika Timor-Leste n.º 1/2004, 14 Abríl 2004.", 11, MUTED)
    s.text(1.2,5.62,10.8,.22,
           "https://mj.gov.tl/jornal/lawsTL/RDTL-Law/RDTL-Gov-Decrees/Gov-Decree-2004-01.pdf", 8, CYAN)
    slides.append(s)
    return slides


def theme_xml() -> str:
    colors = {"dk1": "080D21", "lt1": "FFFFFF", "dk2": "11182E", "lt2": "F4F6FF",
              "accent1": BLUE, "accent2": CYAN, "accent3": PURPLE, "accent4": GOLD,
              "accent5": GREEN, "accent6": CORAL, "hlink": BLUE, "folHlink": PURPLE}
    clr = ''.join(f'<a:{k}><a:srgbClr val="{v}"/></a:{k}>' for k,v in colors.items())
    fills = '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>' * 3
    lines = ''.join('<a:ln w="9525"><a:solidFill><a:schemeClr val="phClr"/></a:solidFill>'
                    '<a:prstDash val="solid"/><a:round/></a:ln>' for _ in range(3))
    effects = '<a:effectStyle><a:effectLst/></a:effectStyle>' * 3
    bg = '<a:solidFill><a:schemeClr val="phClr"/></a:solidFill>' * 3
    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" name="Retailrocket Tetun">'
        '<a:themeElements><a:clrScheme name="Retailrocket">'+clr+'</a:clrScheme>'
        '<a:fontScheme name="Aptos"><a:majorFont><a:latin typeface="Aptos Display"/><a:ea typeface=""/><a:cs typeface=""/></a:majorFont>'
        '<a:minorFont><a:latin typeface="Aptos"/><a:ea typeface=""/><a:cs typeface=""/></a:minorFont></a:fontScheme>'
        '<a:fmtScheme name="Simple"><a:fillStyleLst>'+fills+'</a:fillStyleLst><a:lnStyleLst>'+lines+'</a:lnStyleLst>'
        '<a:effectStyleLst>'+effects+'</a:effectStyleLst><a:bgFillStyleLst>'+bg+'</a:bgFillStyleLst></a:fmtScheme>'
        '</a:themeElements><a:objectDefaults/><a:extraClrSchemeLst/></a:theme>')


def build():
    slides = slide_content()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    overrides = [
        '<Override PartName="/ppt/presentation.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml"/>',
        '<Override PartName="/ppt/slideMasters/slideMaster1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideMaster+xml"/>',
        '<Override PartName="/ppt/slideLayouts/slideLayout1.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slideLayout+xml"/>',
        '<Override PartName="/ppt/theme/theme1.xml" ContentType="application/vnd.openxmlformats-officedocument.theme+xml"/>',
    ] + [f'<Override PartName="/ppt/slides/slide{i}.xml" ContentType="application/vnd.openxmlformats-officedocument.presentationml.slide+xml"/>' for i in range(1,len(slides)+1)]
    content_types = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'+''.join(overrides)+'</Types>')
    slide_ids = ''.join(f'<p:sldId id="{255+i}" r:id="rId{i+2}"/>' for i in range(len(slides)))
    presentation = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<p:presentation xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
        'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main">'
        '<p:sldMasterIdLst><p:sldMasterId id="2147483648" r:id="rId1"/></p:sldMasterIdLst>'
        '<p:sldIdLst>'+slide_ids+'</p:sldIdLst><p:sldSz cx="12192000" cy="6858000" type="screen16x9"/>'
        '<p:notesSz cx="6858000" cy="9144000"/><p:defaultTextStyle/></p:presentation>')
    pres_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" Target="slideMasters/slideMaster1.xml"/>'+
        ''.join(f'<Relationship Id="rId{i+2}" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slide" Target="slides/slide{i+1}.xml"/>' for i in range(len(slides)))+
        '</Relationships>')
    master = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<p:sldMaster xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
        'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main"><p:cSld><p:spTree>'
        '<p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>'
        '<p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>'
        '</p:spTree></p:cSld><p:clrMap bg1="lt1" tx1="dk1" bg2="lt2" tx2="dk2" accent1="accent1" accent2="accent2" accent3="accent3" accent4="accent4" accent5="accent5" accent6="accent6" hlink="hlink" folHlink="folHlink"/>'
        '<p:sldLayoutIdLst><p:sldLayoutId id="1" r:id="rId1"/></p:sldLayoutIdLst><p:txStyles><p:titleStyle/><p:bodyStyle/><p:otherStyle/></p:txStyles></p:sldMaster>')
    master_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" Target="../slideLayouts/slideLayout1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme" Target="../theme/theme1.xml"/>'
        '</Relationships>')
    layout = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<p:sldLayout xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" '
        'xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main" type="blank" preserve="1">'
        '<p:cSld name="Blank"><p:spTree><p:nvGrpSpPr><p:cNvPr id="1" name=""/><p:cNvGrpSpPr/><p:nvPr/></p:nvGrpSpPr>'
        '<p:grpSpPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="0" cy="0"/><a:chOff x="0" y="0"/><a:chExt cx="0" cy="0"/></a:xfrm></p:grpSpPr>'
        '</p:spTree></p:cSld><p:clrMapOvr><a:masterClrMapping/></p:clrMapOvr></p:sldLayout>')
    layout_rels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideMaster" Target="../slideMasters/slideMaster1.xml"/>'
        '</Relationships>')
    with ZipFile(OUTPUT, "w", ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="ppt/presentation.xml"/></Relationships>')
        z.writestr("ppt/presentation.xml", presentation)
        z.writestr("ppt/_rels/presentation.xml.rels", pres_rels)
        z.writestr("ppt/slideMasters/slideMaster1.xml", master)
        z.writestr("ppt/slideMasters/_rels/slideMaster1.xml.rels", master_rels)
        z.writestr("ppt/slideLayouts/slideLayout1.xml", layout)
        z.writestr("ppt/slideLayouts/_rels/slideLayout1.xml.rels", layout_rels)
        z.writestr("ppt/theme/theme1.xml", theme_xml())
        for i, slide in enumerate(slides,1):
            z.writestr(f"ppt/slides/slide{i}.xml", slide.xml())
            z.writestr(f"ppt/slides/_rels/slide{i}.xml.rels", '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/slideLayout" Target="../slideLayouts/slideLayout1.xml"/></Relationships>')
    print(f"Created {OUTPUT} ({len(slides)} slides, {OUTPUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    build()
