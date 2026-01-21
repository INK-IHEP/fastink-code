#!/usr/bin/env python3

from string import Template

png_html="""
<html lang="en">
<head>
	<script type="text/javascript" src="/draw/js/diagram-editor.js"></script>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta charset="UTF-8">
    <title>Drawio for INK</title>
</head>
<body>
  <div style="cursor: pointer; display:inline-block;" title="" onclick="DiagramEditor.editElement(this.firstChild);">
    <img style="display:none;" onload="DiagramEditor.editElement(this);"src="$data"/>
  </div>
</body>
</html>
"""

svg_html="""
<html lang="en">
<head>
	<script type="text/javascript" src="/apps/js/diagram-editor.js"></script>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta charset="UTF-8">
    <title>Drawio for INK</title>
</head>
<body>
  <div style="cursor: pointer; display:inline-block;" title="" onclick="DiagramEditor.editElement(this.firstChild);">
    <object style="pointer-events:none;" onload="DiagramEditor.editElement(this);"
        data="$data">
    </object>
  </div>
</body>
</html>
"""

png_template = Template(png_html)
svg_template = Template(svg_html)
