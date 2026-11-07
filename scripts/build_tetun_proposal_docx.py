"""Render the Tetun proposal Markdown into an editable, styled DOCX."""
from __future__ import annotations

from html import escape
from pathlib import Path
import re
from zipfile import ZIP_DEFLATED, ZipFile

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "proposta_analitika_ecommerce_tetun.md"
OUTPUT = ROOT / "docs" / "proposta_analitika_ecommerce_tetun.docx"


def xml_text(text: str) -> str:
    return escape(text, quote=False)


def clean_inline(text: str) -> str:
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1 — \2", text)
    text = text.replace("**", "").replace("*", "").replace("`", "")
    return text.strip()


def run(text: str, bold: bool = False, italic: bool = False) -> str:
    props = []
    if bold:
        props.append("<w:b/>")
    if italic:
        props.append("<w:i/>")
    rpr = f"<w:rPr>{''.join(props)}</w:rPr>" if props else ""
    return f"<w:r>{rpr}<w:t xml:space=\"preserve\">{xml_text(text)}</w:t></w:r>"


def paragraph(text: str = "", style: str | None = None, *, bold: bool = False,
              italic: bool = False, page_break: bool = False) -> str:
    ppr = []
    if style:
        ppr.append(f'<w:pStyle w:val="{style}"/>')
    if page_break:
        ppr.append("<w:pageBreakBefore/>")
    props = f"<w:pPr>{''.join(ppr)}</w:pPr>" if ppr else ""
    body = run(clean_inline(text), bold=bold, italic=italic) if text else ""
    return f"<w:p>{props}{body}</w:p>"


def table(rows: list[list[str]]) -> str:
    if not rows:
        return ""
    col_count = max(len(row) for row in rows)
    widths = [int(9360 / col_count)] * col_count
    grid = "".join(f'<w:gridCol w:w="{width}"/>' for width in widths)
    tr_xml = []
    for ri, row in enumerate(rows):
        cells = []
        for ci in range(col_count):
            text = clean_inline(row[ci]) if ci < len(row) else ""
            shading = '<w:shd w:fill="123B67"/>' if ri == 0 else '<w:shd w:fill="F3F6FA"/>' if ri % 2 else ''
            color = '<w:color w:val="FFFFFF"/>' if ri == 0 else '<w:color w:val="1E293B"/>'
            cells.append(
                '<w:tc><w:tcPr><w:tcW w:w="%d" w:type="dxa"/>%s'
                '<w:tcMar><w:top w:w="100" w:type="dxa"/><w:bottom w:w="100" w:type="dxa"/>'
                '<w:start w:w="110" w:type="dxa"/><w:end w:w="110" w:type="dxa"/></w:tcMar></w:tcPr>'
                '<w:p><w:pPr><w:spacing w:after="50"/></w:pPr><w:r><w:rPr>%s%s</w:rPr>'
                '<w:t xml:space="preserve">%s</w:t></w:r></w:p></w:tc>'
                % (widths[ci], shading, '<w:b/>' if ri == 0 else '', color, xml_text(text)))
        trpr = '<w:trPr><w:tblHeader/></w:trPr>' if ri == 0 else ''
        tr_xml.append(f'<w:tr>{trpr}{"".join(cells)}</w:tr>')
    return ('<w:tbl><w:tblPr><w:tblW w:w="9360" w:type="dxa"/>'
            '<w:tblLayout w:type="fixed"/><w:tblBorders>'
            '<w:top w:val="single" w:sz="5" w:color="CBD5E1"/>'
            '<w:left w:val="single" w:sz="5" w:color="CBD5E1"/>'
            '<w:bottom w:val="single" w:sz="5" w:color="CBD5E1"/>'
            '<w:right w:val="single" w:sz="5" w:color="CBD5E1"/>'
            '<w:insideH w:val="single" w:sz="4" w:color="D9E1EA"/>'
            '<w:insideV w:val="single" w:sz="4" w:color="D9E1EA"/>'
            '</w:tblBorders></w:tblPr><w:tblGrid>' + grid + '</w:tblGrid>' + ''.join(tr_xml) + '</w:tbl>')


def diagram(block: list[str]) -> str:
    """Render an editable, compact process diagram from node/flow rows."""
    nodes = []
    for line in block:
        bits = line.split('|', 3)
        if len(bits) == 4 and bits[0] == 'node':
            nodes.append((bits[1], bits[2], bits[3]))
        elif len(bits) == 2 and bits[0] == 'flow':
            nodes.append(('', '↓  ' + bits[1], ''))
    if not nodes:
        return ''
    widths = [1100, 2500, 5760]
    grid = ''.join(f'<w:gridCol w:w="{w}"/>' for w in widths)
    rows = []
    for index, (number, title, description) in enumerate(nodes):
        if not number:
            content = '↓  ' + title.removeprefix('↓  ')
            rows.append('<w:tr><w:tc><w:tcPr><w:tcW w:w="1100" w:type="dxa"/><w:shd w:fill="EAF5F8"/></w:tcPr><w:p/></w:tc>'
                        '<w:tc><w:tcPr><w:gridSpan w:val="2"/><w:shd w:fill="EAF5F8"/></w:tcPr>'
                        f'<w:p><w:pPr><w:jc w:val="center"/><w:spacing w:before="40" w:after="40"/></w:pPr><w:r><w:rPr><w:b/><w:color w:val="087E8B"/><w:sz w:val="18"/></w:rPr><w:t>{xml_text(content)}</w:t></w:r></w:p></w:tc></w:tr>')
            continue
        left_fill = ['123B67','087E8B','51468A','176B87','276749','9A5B13'][index % 6]
        cells = []
        values = [number, title, description]
        for ci, (value, width) in enumerate(zip(values, widths)):
            fill = left_fill if ci < 2 else ('F3F7FB' if index % 2 == 0 else 'FFFFFF')
            color = 'FFFFFF' if ci < 2 else '243247'
            bold = '<w:b/>' if ci < 2 else ''
            size = '21' if ci == 1 else ('20' if ci == 0 else '19')
            cells.append(f'<w:tc><w:tcPr><w:tcW w:w="{width}" w:type="dxa"/><w:shd w:fill="{fill}"/>'
                         '<w:tcMar><w:top w:w="110" w:type="dxa"/><w:bottom w:w="110" w:type="dxa"/>'
                         '<w:start w:w="130" w:type="dxa"/><w:end w:w="130" w:type="dxa"/></w:tcMar></w:tcPr>'
                         f'<w:p><w:pPr><w:spacing w:after="30"/></w:pPr><w:r><w:rPr>{bold}<w:color w:val="{color}"/><w:sz w:val="{size}"/></w:rPr>'
                         f'<w:t xml:space="preserve">{xml_text(value)}</w:t></w:r></w:p></w:tc>')
        rows.append('<w:tr>' + ''.join(cells) + '</w:tr>')
    return ('<w:tbl><w:tblPr><w:tblW w:w="9360" w:type="dxa"/><w:tblLayout w:type="fixed"/>'
            '<w:tblBorders><w:top w:val="single" w:sz="6" w:color="B8C7D8"/><w:left w:val="single" w:sz="6" w:color="B8C7D8"/>'
            '<w:bottom w:val="single" w:sz="6" w:color="B8C7D8"/><w:right w:val="single" w:sz="6" w:color="B8C7D8"/>'
            '<w:insideH w:val="single" w:sz="4" w:color="D9E1EA"/><w:insideV w:val="single" w:sz="4" w:color="FFFFFF"/>'
            '</w:tblBorders></w:tblPr><w:tblGrid>' + grid + '</w:tblGrid>' + ''.join(rows) + '</w:tbl>')


def render_body(markdown: str) -> str:
    lines = markdown.splitlines()
    out: list[str] = []
    in_code = False
    code_lines: list[str] = []
    table_lines: list[str] = []
    in_diagram = False
    diagram_lines: list[str] = []
    cover = True
    i = 0

    def flush_table():
        nonlocal table_lines
        if table_lines:
            parsed = []
            for row in table_lines:
                cols = [c.strip() for c in row.strip().strip('|').split('|')]
                if all(re.fullmatch(r':?-{3,}:?', c) for c in cols):
                    continue
                parsed.append(cols)
            out.append(table(parsed))
            out.append(paragraph())
            table_lines = []

    while i < len(lines):
        line = lines[i]
        if line.strip() == ':::diagram':
            flush_table()
            in_diagram = True
            diagram_lines = []
            i += 1
            continue
        if in_diagram:
            if line.strip() == ':::':
                out.append(diagram(diagram_lines))
                out.append(paragraph())
                in_diagram = False
                diagram_lines = []
            else:
                diagram_lines.append(line)
            i += 1
            continue
        if line.startswith('```'):
            flush_table()
            if in_code:
                out.extend(paragraph(x, 'CodeBlock') for x in code_lines)
                code_lines = []
                in_code = False
            else:
                in_code = True
            i += 1
            continue
        if in_code:
            code_lines.append(line)
            i += 1
            continue
        if line.startswith('|'):
            table_lines.append(line)
            i += 1
            continue
        flush_table()
        if line.startswith('# '):
            title = clean_inline(line[2:])
            if title.startswith('Kap'):
                cover = False
                out.append(paragraph(title, 'Heading1', page_break=True))
            elif title.startswith(('Estrutura ', 'Refer', 'Ap')):
                out.append(paragraph(title, 'Heading1', page_break=True))
            else:
                out.append(paragraph(title, 'Title'))
        elif line.startswith('## '):
            title = clean_inline(line[3:])
            style = 'Subtitle' if cover and title.startswith('Analítika ') else 'Heading2'
            out.append(paragraph(title, style))
        elif line.startswith('### '):
            out.append(paragraph(clean_inline(line[4:]), 'Heading3'))
        elif line.strip() == '---':
            out.append(paragraph())
        elif line.startswith('- '):
            out.append(paragraph('\u2022 ' + line[2:], 'ListBullet'))
        elif re.match(r'^\d+\.\s', line):
            out.append(paragraph(re.match(r'^\d+\.', line).group(0) + ' ' + re.sub(r'^\d+\.\s+', '', line), 'ListNumber'))
        elif line.startswith('**') and line.endswith('**'):
            out.append(paragraph(line, 'Metadata', bold=True))
        elif line.strip():
            out.append(paragraph(line, 'Normal'))
        i += 1
    flush_table()
    if in_code:
        out.extend(paragraph(x, 'CodeBlock') for x in code_lines)
    return ''.join(out)


def styles_xml() -> str:
    return '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
 <w:docDefaults><w:rPrDefault><w:rPr><w:rFonts w:ascii="Aptos" w:hAnsi="Aptos" w:eastAsia="Aptos"/><w:sz w:val="22"/><w:color w:val="243247"/></w:rPr></w:rPrDefault><w:pPrDefault><w:pPr><w:spacing w:after="140" w:line="290" w:lineRule="auto"/></w:pPr></w:pPrDefault></w:docDefaults>
 <w:style w:type="paragraph" w:default="1" w:styleId="Normal"><w:name w:val="Normal"/><w:rPr><w:rFonts w:ascii="Aptos" w:hAnsi="Aptos"/><w:sz w:val="22"/><w:color w:val="243247"/></w:rPr></w:style>
 <w:style w:type="paragraph" w:styleId="Title"><w:name w:val="Title"/><w:pPr><w:jc w:val="center"/><w:spacing w:before="2200" w:after="260"/></w:pPr><w:rPr><w:rFonts w:ascii="Aptos Display" w:hAnsi="Aptos Display"/><w:b/><w:sz w:val="40"/><w:color w:val="123B67"/></w:rPr></w:style>
 <w:style w:type="paragraph" w:styleId="Subtitle"><w:name w:val="Subtitle"/><w:pPr><w:jc w:val="center"/><w:spacing w:after="260"/></w:pPr><w:rPr><w:b/><w:sz w:val="30"/><w:color w:val="176B87"/></w:rPr></w:style>
 <w:style w:type="paragraph" w:styleId="Heading1"><w:name w:val="heading 1"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:pPr><w:keepNext/><w:spacing w:before="180" w:after="220"/><w:outlineLvl w:val="0"/></w:pPr><w:rPr><w:rFonts w:ascii="Aptos Display" w:hAnsi="Aptos Display"/><w:b/><w:sz w:val="32"/><w:color w:val="123B67"/></w:rPr></w:style>
 <w:style w:type="paragraph" w:styleId="Heading2"><w:name w:val="heading 2"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:pPr><w:keepNext/><w:spacing w:before="260" w:after="100"/><w:outlineLvl w:val="1"/></w:pPr><w:rPr><w:b/><w:sz w:val="25"/><w:color w:val="176B87"/></w:rPr></w:style>
 <w:style w:type="paragraph" w:styleId="Heading3"><w:name w:val="heading 3"/><w:basedOn w:val="Normal"/><w:next w:val="Normal"/><w:pPr><w:keepNext/><w:spacing w:before="170" w:after="70"/><w:outlineLvl w:val="2"/></w:pPr><w:rPr><w:b/><w:sz w:val="23"/><w:color w:val="51468A"/></w:rPr></w:style>
 <w:style w:type="paragraph" w:styleId="Metadata"><w:name w:val="Metadata"/><w:pPr><w:jc w:val="center"/><w:spacing w:before="80" w:after="100"/></w:pPr><w:rPr><w:sz w:val="21"/><w:color w:val="334155"/></w:rPr></w:style>
 <w:style w:type="paragraph" w:styleId="ListBullet"><w:name w:val="List Bullet"/><w:basedOn w:val="Normal"/><w:pPr><w:ind w:left="420" w:hanging="220"/><w:spacing w:after="80"/></w:pPr></w:style>
 <w:style w:type="paragraph" w:styleId="ListNumber"><w:name w:val="List Number"/><w:basedOn w:val="Normal"/><w:pPr><w:ind w:left="420" w:hanging="220"/><w:spacing w:after="80"/></w:pPr></w:style>
 <w:style w:type="paragraph" w:styleId="CodeBlock"><w:name w:val="Code Block"/><w:pPr><w:shd w:fill="F1F5F9"/><w:spacing w:after="0"/><w:ind w:left="220" w:right="140"/></w:pPr><w:rPr><w:rFonts w:ascii="Consolas" w:hAnsi="Consolas"/><w:sz w:val="17"/><w:color w:val="1E3A5F"/></w:rPr></w:style>
</w:styles>'''


def build() -> None:
    markdown = SOURCE.read_text(encoding="utf-8")
    body = render_body(markdown)
    sect = '''<w:sectPr><w:headerReference w:type="default" r:id="rId1"/><w:footerReference w:type="default" r:id="rId2"/><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1134" w:right="1276" w:bottom="1134" w:left="1276" w:header="567" w:footer="567" w:gutter="0"/></w:sectPr>'''
    document = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<w:body>' + body + sect + '</w:body></w:document>')
    header = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:hdr xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        '<w:p><w:pPr><w:pBdr><w:bottom w:val="single" w:sz="5" w:space="5" w:color="CBD5E1"/></w:pBdr>'
        '<w:jc w:val="right"/></w:pPr><w:r><w:rPr><w:sz w:val="16"/><w:color w:val="64748B"/></w:rPr>'
        '<w:t>Proposta Analitika Komérsiu Elektróniku</w:t></w:r></w:p></w:hdr>')
    footer = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:ftr xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        '<w:p><w:pPr><w:jc w:val="center"/></w:pPr><w:r><w:rPr><w:color w:val="64748B"/></w:rPr>'
        '<w:fldChar w:fldCharType="begin"/></w:r><w:r><w:instrText xml:space="preserve"> PAGE </w:instrText></w:r>'
        '<w:r><w:fldChar w:fldCharType="end"/></w:r></w:p></w:ftr>')
    content_types = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
<Override PartName="/word/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>
<Override PartName="/word/header1.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.header+xml"/>
<Override PartName="/word/footer1.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml"/>
</Types>'''
    root_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/></Relationships>'''
    doc_rels = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/header" Target="header1.xml"/>
<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/footer" Target="footer1.xml"/>
<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>'''
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(OUTPUT, "w", ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("word/document.xml", document)
        archive.writestr("word/styles.xml", styles_xml())
        archive.writestr("word/header1.xml", header)
        archive.writestr("word/footer1.xml", footer)
        archive.writestr("word/_rels/document.xml.rels", doc_rels)
    print(f"Created {OUTPUT} ({OUTPUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    build()
