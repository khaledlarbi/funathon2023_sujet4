dict_corresp_color = {
    "green": "#7cb342", "🟢": "#7cb342",
    "yellow": "#ffcc32", "🟡": "#ffcc32",
    "blue" : "#1976d2", "🔵" : "#1976d2",
    "red": "#f44336", "🔴": "#f44336",
    "black": "#424242", "⚫": "#424242"
}

def create_box_level(
    color = "green",
    title = "Hint"
):
    hex_color = dict_corresp_color[color]
    div_specif = f'<div class="alert alert-warning" role="alert" style="color: rgba(0,0,0,.8); background-color: white; margin-top: 1em; margin-bottom: 1em; margin:1.5625emauto; padding:0 .6rem .8rem!important;overflow:hidden; page-break-inside:avoid; border-radius:.25rem; box-shadow:0 .2rem .5rem rgba(0,0,0,.05),0 0 .05rem rgba(0,0,0,.1); transition:color .25s,background-color .25s,border-color .25s ; border-right: 1px solid #dee2e6 ; border-top: 1px solid #dee2e6 ; border-bottom: 1px solid #dee2e6 ; border-left:.2rem solid {hex_color};">'
    div_header = f'<h3 class="alert-heading"><i class="fa fa-pencil"></i> {title}</h3>'
    div_to_print = div_specif + "\n" + div_header
    print(div_to_print)