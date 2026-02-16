from fasthtml.common import *
from backend import (search_serials,
                    bronze_paths_for_serial,
                    zip_csv_from_parquets,
                    filter_bronze_by_testtype,
                    search_teststand)
from fastapi import Response
app, rt = fast_app()

# ---------- Utilities ----------

#Availiable test types for bronze - default is MAT software with all csvs displayed
def parse_selected(csv: str) -> list[str]:
    if not csv:
        return []
    seen = set()
    out = []
    for s in csv.split(","):
        s = s.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out

def csv_selected(lst: list[str]) -> str:
    return ",".join(lst)

# ---------- Left list renderer (multi-select) ----------
def render_stand_list(all_matches: list[str], selected_order: list[str], oob: bool = False):
    sel_set = set(selected_order)
    ordered = list(selected_order) + [s for s in all_matches if s not in sel_set]

    def btn_style(s):
        base = (
            "width:100%; text-align:center; background:#fff; color:#111;"
            "padding:10px 12px; border-radius:10px; border:none; cursor:pointer;"
        )
        return base if s not in sel_set else base.replace(
            "background:#fff; color:#111;", "background:#fde047; color:#111;"
        )

    ul = Ul(
        *[
            Li(
                Button(
                    s,
                    style=btn_style(s),
                    hx_get=f"/api/toggle_stand?stand={s}",
                    hx_include="#test-stand, #stand-selected, #serial, #selected, #pselected, #module-current, #test-type",
                    hx_target="#stand-results",
                    hx_swap="innerHTML",
                )
            )
            for s in ordered
        ],
        id="stand-results",
        style=(
            "list-style:none; padding:0; margin:0; display:flex; "
            "flex-direction:column; gap:8px; align-items:center;"
        ),
    )
    if oob:
        ul.attrs["hx-swap-oob"] = "true"
    return ul


def render_serial_list(all_matches: list[str], selected_order: list[str], oob: bool = False):
    """
    - Selected items are rendered on top in the order of `selected_order`.
    - Non-selected follow in the original search order.
    - Selected are highlighted; clicking toggles selection.
    """
    sel_set = set(selected_order)
    ordered = list(selected_order) + [s for s in all_matches if s not in sel_set]

    def btn_style(s):
        base = (
            "width:100%; text-align:center; background:#fff; color:#111;"
            "padding:10px 12px; border-radius:10px; border:none; cursor:pointer;"
        )
        return base if s not in sel_set else base.replace(
            "background:#fff; color:#111;", "background:#fde047; color:#111;"
        )

    ul = Ul(
        *[
            Li(
                Button(
                    s,
                    style=btn_style(s),
                    # Toggle selection; include current filter and selected-state
                    hx_get=f"/api/toggle?serial={s}",
                    hx_include="#serial, #selected, #pselected, #module-current, #test-type, #stand-selected",
                    hx_target="#bronze-col",
                    hx_swap="outerHTML",
                )
            )
            for s in ordered
        ],
        id="results",
        style=("list-style:none; padding:0; margin:0; display:flex; "
               "flex-direction:column; gap:8px; align-items:center;"),
    )
    if oob:
        ul.attrs["hx-swap-oob"] = "true"
    return ul

# ---------- Right panel renderer (grouped by serial, sorted) ----------
TEST_TYPES = {
    "MAT": ["Stability", "Final"],
    "DVT": [],  
}

def render_bronze_panel(
    selected_order: list[str],
    psel: list[str],
    module: str = "MAT",
    test_types: list[str] | None = None,
):
    if not selected_order:
        # Hide panel (same styling as before)
        return Div(
            # Keep parquet selection state around as hidden input
            Input(type="hidden", id="pselected", name="pselected", value=""),
            Div("Select a serial to view Bronze file paths.", style="opacity:.8;"),
            id="bronze-col",
            style="display:none; width:100%; max-width:560px;",
            hx_swap_oob="true",
        )

    # --- normalize module + test types ---
    module = (module or "MAT").upper()
    if module not in TEST_TYPES:
        module = "MAT"

    allowed_types = TEST_TYPES.get(module, [])
    if test_types is not None:
        test_types = [t for t in (test_types or []) if t in allowed_types]

    # None / [] => no filtering
    active_types = test_types if test_types else None

    sel_paths = set(psel or [])
    # Sorted by serial number for display (requirement)
    for_display = sorted(selected_order, key=lambda s: s)

    # Fetch per-serial paths
    groups = []
    all_paths: list[str] = []
    for s in for_display:
        try:
            paths = bronze_paths_for_serial(s, limit=500)
            #apply parquet metadata filter if test types are active
            if active_types:
                paths = filter_bronze_by_testtype(paths, active_types)
        except Exception as e:
            paths = [f"(DB error: {e})"]

        # Collect non-error paths for the "Download all" button
        for p in paths:
            if not str(p).startswith("(DB error"):
                all_paths.append(str(p))

        items = []
        for p in paths:
            p_str = str(p)
            p_str = p_str.removeprefix("parquets\\")
            if p_str.startswith("(DB error"):
                items.append(Li(Code(p_str)))
                continue

            base_style = (
                "width:100%; text-align:left; background:transparent; color:inherit;"
                "padding:0; border:none; cursor:pointer;"
            )
            if p_str in sel_paths:
                base_style += " background:#fde047; color:#111; padding:4px 6px; border-radius:6px;"

            items.append(
                Li(
                    Button(
                        Code(p_str),
                        style=base_style,
                        hx_get=f"/api/toggle_parquet?path={p_str}",
                        hx_include="#serial, #selected, #pselected, #module-current, #test-type",
                        hx_target="#bronze-col",
                        hx_swap="outerHTML",
                    )
                )
            )

        groups.append(
            Div(
                Div(s, style="font-weight:600; margin:4px 0 6px 0;"),
                (Div("No bronze paths found.", style="opacity:.85;")
                 if not paths else
                 Ul(
                     *items,
                     style=(
                         "list-style:none; padding:0; margin:0; display:flex; "
                         "flex-direction:column; gap:6px;"
                     ),
                 )),
                style="text-align:left;"
            )
        )

    # Download buttons
    controls = Div(
        # Download ALL parquets in view as CSVs (zipped)
        Form(
            Input(type="hidden", name="paths", value=",".join(all_paths)),
            Button(
                "Download all CSVs",
                type="submit",
                style=(
                    "padding:8px 10px; border-radius:10px; border:1px solid rgba(255,255,255,.35); "
                    "background:transparent; color:#fff; cursor:pointer; margin-right:8px;"
                ),
            ),
            method="get",
            action="/download/csv_zip",
            target="_blank",
        ),
        # Download ONLY selected parquets as CSVs (zipped)
        Form(
            Input(type="hidden", name="paths", value=",".join(psel or [])),
            Button(
                "Download selected CSVs",
                type="submit",
                disabled=(len(psel or []) == 0),
                style=(
                    "padding:8px 10px; border-radius:10px; border:1px solid rgba(255,255,255,.35); "
                    "background:transparent; color:#fff; cursor:pointer;"
                    + ("" if psel else " opacity:.6; cursor:not-allowed;")
                ),
            ),
            method="get",
            action="/download/csv_zip",
            target="_blank",
        ),
        style=(
            "display:flex; justify-content:flex-start; align-items:center; margin-bottom:10px; "
            "gap:8px; flex-wrap:wrap;"
        ),
    )

    # Keep original container styling
    return Div(
        Div(
            Div(f"Bronze files for {len(selected_order)} selected", style="font-weight:600;"),
            controls,
            style=(
                "display:flex; justify-content:space-between; align-items:center; "
                "margin-bottom:10px; gap:12px; flex-wrap:wrap;"
            ),
        ),
        Div(
            *groups,
            style=(
                "background:rgba(255,255,255,.08); border:1px solid rgba(255,255,255,.22); "
                "padding:10px; border-radius:12px; color:#fff;"
            )
        ),
        # Hidden state for selected parquet paths
        Input(type="hidden", id="pselected", name="pselected", value=",".join(psel or [])),
        id="bronze-col",
        style="display:block; width:100%; max-width:560px;",
        hx_swap_oob="true",
    )

# ---------- Module + Test Type buttons ----------
def render_module_panel(current_module: str = "MAT", selected_types: list[str] | None = None):
    current_module = (current_module or "MAT").upper()
    if current_module not in TEST_TYPES:
        current_module = "MAT"

    allowed = TEST_TYPES[current_module]
    if selected_types is None:
        selected_types = list(allowed)
    else:
        selected_types = [t for t in (selected_types or []) if t in allowed]

    selected_set = set(selected_types)

    def module_btn(label: str):
        is_sel = (label.upper() == current_module)
        base = (
            "flex:1; padding:12px; font-size:16px; border-radius:10px; "
            "border:1px solid #ddd; cursor:pointer; text-align:center; "
            "background:#fff; color:#111;"
        )
        if is_sel:
            base = base.replace("background:#fff; color:#111;", "background:#1d4ed8; color:#fff;")
        return Button(
            label,
            type="button",
            style=base,
            hx_get=f"/api/module_select?module={label}",
            hx_include="#test-type, #selected, #pselected, #serial",
            hx_target="#module-testtype-panel",
            hx_swap="outerHTML",
        )

    def test_btn(tt: str):
        is_sel = tt in selected_set
        base = (
            "flex:1; padding:12px; font-size:16px; border-radius:10px; "
            "border:1px solid #ddd; cursor:pointer; text-align:center; min-width:0; "
            "background:#fff; color:#111;"
        )
        if is_sel:
            base = base.replace("background:#fff; color:#111;", "background:#22c55e; color:#111;")
        return Button(
            tt,
            type="button",
            style=base,
            hx_get=f"/api/testtype_toggle?tt={tt}",
            hx_include="#module-current, #test-type, #selected, #pselected, #serial",
            hx_target="#module-testtype-panel",
            hx_swap="outerHTML",
        )

    if allowed:
        types_row = Div(
            *[test_btn(tt) for tt in allowed],
            style="display:flex; gap:10px; width:100%; max-width:560px; flex-wrap:wrap;",
        )
    else:
        types_row = Div(
            "No test types configured for this module yet.",
            style="opacity:.8; text-align:left; padding:4px 0;",
        )

    return Div(
        Input(type="hidden", id="module-current", name="module", value=current_module),
        Input(type="hidden", id="test-type", name="test_type", value=csv_selected(selected_types)),
        Div(
            module_btn("MAT"),
            module_btn("DVT"),
            style="display:flex; gap:10px; width:100%; max-width:560px; margin-bottom:10px;",
        ),
        types_row,
        id="module-testtype-panel",
        style="display:flex; flex-direction:column; gap:10px; width:100%;",
    )




def nav_links():
    return Div(
        A("Home", href="/", style="text-decoration:none; color:rgba(255,255,255,.85);"),
        A("Modules", href="/modules", style="text-decoration:none; color:rgba(255,255,255,.85);"),
        style=(
            "position:fixed; top:8px; left:16px;"
            "font-size:16px; display:flex; gap:12px;"
            "opacity:.9;"
        ),
    )
# ---------- Page ----------
def page():
    layout_style = Style(
        """
        :root{--grid: minmax(320px,560px); --justify:center;}
        #columns{
            display:grid; gap:16px; grid-template-columns: var(--grid);
            justify-items: var(--justify);
        }
        """,
        id="layout-state"
    )
    return Div(
        layout_style,
        nav_links(),
        Div(
            H1("Filter Data", style="margin:0 0 12px 0; font-size:36px;"),
            Div(
                # LEFT column: test stand + serial
                Div(
                    Input(
                        id="test-stand",
                        name="test_stand",
                        placeholder="Type a test stand…",
                        autocomplete="off",
                        style=(
                            "flex:1; padding:12px; font-size:16px; border-radius:10px; "
                            "border:1px solid #ddd; width:100%; max-width:560px;"
                        ),
                        hx_get="/api/teststands",
                        hx_trigger="keyup changed delay:200ms",
                        hx_target="#stand-results",
                        hx_swap="innerHTML",
                        hx_include="#stand-selected, #selected, #pselected, #module-current, #test-type",
                    ),
                    Input(
                        id="serial",
                        name="q", 
                        placeholder="Type a serial number…",
                        autocomplete="off",
                        style=(
                            "flex:1; padding:12px; font-size:16px; border-radius:10px; "
                            "border:1px solid #ddd; width:100%; max-width:560px;"
                        ),
                        hx_get="/api/serials", 
                        hx_trigger="keyup changed delay:200ms",
                        hx_target="#results", 
                        hx_swap="innerHTML",
                        # Include selection state while typing so selected items stay at top
                        hx_include="#stand-selected, #selected, #pselected, #module-current, #test-type",
                    ),
                    style="display:flex; flex-direction:column; gap:10px; width:100%;",
                ),

                # RIGHT column: module + test-type toggle buttons
                Div(
                    render_module_panel("MAT", ["Stability", "Final"]),
                    style="display:flex; flex-direction:column; gap:10px; width:100%;",
                ),

                # Grid container for the 2x2 layout
                style=(
                    "display:grid; grid-template-columns:repeat(2, minmax(0, 1fr));"
                    "gap:12px; width:100%; max-width:560px; margin:0 auto 16px auto;"
                ),
            ),

            # Hidden state for selected serial numbers (CSV)
            Input(type="hidden", id="selected", name="selected", value=""),
            Input(type="hidden", id="stand-selected", name="stand_selected", value=""),

            Div(
                # LEFT column (serial list)
                Div(
                    Div(
                        Div("Start typing to see stands…", id="stand-results", style="opacity:.85;"),
                        Div("Start typing to see matches…", id="results", style="opacity:.85;"),
                        style=(
                            "display:grid; grid-template-columns:repeat(2, minmax(0, 1fr)); "
                            "gap:8px; align-items:flex-start;"
                        ),
                    ),
                ),
                
                # RIGHT column (bronze list) hidden initially (unchanged styling)
                render_bronze_panel([], []),
                id="columns",
                style="text-align:center;"
            ),
            
        ),
        style=(
            "min-height:100vh; margin:0; padding:24px;"
            "display:grid; place-items:center; text-align:center;"
            "font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;"
            "background:linear-gradient(135deg,#38bdf8,#1e3a8a); color:white;"
        ),
    )

@rt("/")
def home():
    return page()

# ---------- Search: keep selected at top even if filter hides them ----------
@rt("/api/serials")
def api_serials(selected: str = "", q: str = "", stand_selected: str = ""):
    q = (q or "").strip()
    sel = parse_selected(selected)
    stand_sel = parse_selected(stand_selected)

    if not q:
        # Show only selected if nothing typed? Keep original behavior:
        # "Start typing..." but still keep selected visible for removal.
        serials = []
    else:
        try:
            serials = search_serials(prefix=q, limit=50, test_stands=stand_sel or None)
        except Exception as e:
            return Div(f"DB error: {e}", style="opacity:.85;", id="results")

    # Merge so selected are always visible at top
    union = sel + [s for s in serials if s not in set(sel)]
    if not q and not sel:
        return Div("Start typing to see matches…", style="opacity:.85;", id="results")

    return render_serial_list(union, selected_order=sel, oob=False)

@rt("/api/teststands")
def api_teststands(stand_selected: str = "", test_stand: str = ""):
    tq = (test_stand or "").strip()
    sel_stands = parse_selected(stand_selected)

    if not tq:
        stands = []
    else:
        try:
            stands = search_teststand(prefix=tq, limit=50)
        except Exception as e:
            return Div(f"DB error: {e}", style="opacity:.85;", id="stand-results")

    union = sel_stands + [s for s in stands if s not in set(sel_stands)]
    if not tq and not sel_stands:
        return Div("Start typing to see stands…", style="opacity:.85;", id="stand-results")

    return render_stand_list(union, selected_order=sel_stands, oob=False)

@rt("/api/toggle_stand")
def api_toggle_stand(
    stand: str = "",
    stand_selected: str = "",
    selected: str = "",
    pselected: str = "",
    test_stand: str = "",
    q: str = "",
    module: str = "MAT",
    test_type: str = "",
):
    st = (stand or "").strip()
    stand_sel = parse_selected(stand_selected)
    sel = parse_selected(selected)
    psel = parse_selected(pselected)

    # Toggle stand in selection (move newly-added to the front)
    if st:
        if st in stand_sel:
            stand_sel = [x for x in stand_sel if x != st]
        else:
            stand_sel = [st] + [x for x in stand_sel if x != st]

    # Recompute stand list based on current stand search text
    tq = (test_stand or "").strip()
    try:
        stand_matches = search_teststand(prefix=tq, limit=50) if tq else []
    except Exception:
        stand_matches = []

    stand_union = stand_sel + [x for x in stand_matches if x not in set(stand_sel)]
    stand_list = render_stand_list(stand_union, selected_order=stand_sel, oob=True)

    # Keep stand selection state in a hidden input (OOB)
    stand_selected_input = Input(
        type="hidden",
        id="stand-selected",
        name="stand_selected",
        value=csv_selected(stand_sel),
        hx_swap_oob="true",
    )

    # Recompute serial list respecting selected stands
    q = (q or "").strip()
    try:
        serial_matches = (
            search_serials(prefix=q, limit=50, test_stands=stand_sel or None) if q else []
        )
    except Exception:
        serial_matches = []

    serial_union = sel + [x for x in serial_matches if x not in set(sel)]
    serial_list = render_serial_list(serial_union, selected_order=sel, oob=True)

    # We don't touch the bronze panel or layout here (serial selection not changed)
    return Div(stand_selected_input, stand_list, serial_list)



# ---------- Toggle selection + update both panels ----------
@rt("/api/toggle_parquet")
def api_toggle_parquet(path: str = "", selected: str = "", pselected: str = "", module: str = "MAT", test_type: str = ""):
    p = (path or "").strip()
    sel = parse_selected(selected)
    psel = parse_selected(pselected)

    if p:
        if p in psel:
            psel = [x for x in psel if x != p]
        else:
            psel = [p] + [x for x in psel if x != p]

    module = (module or "MAT").upper()
    if module not in TEST_TYPES:
        module = "MAT"
    types = [t for t in parse_selected(test_type) if t in TEST_TYPES.get(module, [])]

    right_panel = render_bronze_panel(sel, psel, module, types)
    return right_panel


@rt("/api/toggle")
def api_toggle(
    serial: str = "",
    selected: str = "",
    pselected: str = "",
    stand_selected: str = "",
    q: str = "",
    module: str = "MAT",
    test_type: str = "",
):
    s = (serial or "").strip()
    sel = parse_selected(selected)
    psel = parse_selected(pselected)
    stand_sel = parse_selected(stand_selected)

    # Toggle s in the selection list; newly-added goes to the front (move-to-top)
    if s:
        if s in sel:
            sel = [x for x in sel if x != s]
        else:
            sel = [s] + [x for x in sel if x != s]

    # Recompute left list content based on current filter `q`
    q = (q or "").strip()
    try:
        matches = search_serials(prefix=q, limit=50, test_stands=stand_sel or None) if q else []
    except Exception:
        matches = []

    union = sel + [x for x in matches if x not in set(sel)]
    left_list = render_serial_list(union, selected_order=sel, oob=True)

    # Update hidden selected state (OOB)
    selected_input = Input(
        type="hidden",
        id="selected",
        name="selected",
        value=csv_selected(sel),
        hx_swap_oob="true",
    )

    #normalize module + test types and pass into render_bronze_panel
    module = (module or "MAT").upper()
    if module not in TEST_TYPES:
        module = "MAT"
    types = [t for t in parse_selected(test_type) if t in TEST_TYPES.get(module, [])]

    # Update right panel (OOB), grouped + sorted by serial number, filtered by module/types
    right_panel = render_bronze_panel(sel, psel, module, types)

    # Switch layout between one/two columns (same styles as before)
    style_block = (
        Style(
            """
            :root{--grid: minmax(320px,300px) minmax(320px,300px); --justify:start;}
            #columns{
                display:grid; gap:16px; grid-template-columns: var(--grid);
                justify-items: var(--justify);
            }
            """,
            id="layout-state",
            hx_swap_oob="true",
        )
        if sel
        else Style(
            """
            :root{--grid: minmax(320px,560px); --justify:center;}
            #columns{
                display:grid; gap:16px; grid-template-columns: var(--grid);
                justify-items: var(--justify);
            }
            """,
            id="layout-state",
            hx_swap_oob="true",
        )
    )

    # Return only OOB fragments so the columns themselves never reflow
    return Div(selected_input, right_panel, style_block, left_list)


@rt("/api/module_select")
def api_module_select(
    module: str = "MAT",
    selected: str = "",
    pselected: str = "",
    test_type: str = "",
):
    module = (module or "MAT").upper()
    if module not in TEST_TYPES:
        module = "MAT"

    sel = parse_selected(selected)
    psel = parse_selected(pselected)

    # When changing module, default to "all" test types for that module
    selected_types = TEST_TYPES.get(module, [])

    module_panel = render_module_panel(module, selected_types)
    right_panel = render_bronze_panel(sel, psel, module, selected_types)

    # hx-target is #module-testtype-panel; bronze panel updates via hx-swap-oob
    return Div(module_panel, right_panel)


@rt("/api/testtype_toggle")
def api_testtype_toggle(
    tt: str = "",
    module: str = "MAT",
    test_type: str = "",
    selected: str = "",
    pselected: str = "",
):
    module = (module or "MAT").upper()
    if module not in TEST_TYPES:
        module = "MAT"

    allowed = TEST_TYPES.get(module, [])
    current_selected = [t for t in parse_selected(test_type) if t in allowed]

    tt = (tt or "").strip()
    if tt and tt in allowed:
        if tt in current_selected:
            current_selected = [x for x in current_selected if x != tt]
        else:
            current_selected.append(tt)

    sel = parse_selected(selected)
    psel = parse_selected(pselected)

    module_panel = render_module_panel(module, current_selected)
    right_panel = render_bronze_panel(sel, psel, module, current_selected)

    return Div(module_panel, right_panel)

@rt("/download/csv_zip")
def download_csv_zip(paths: str = ""):
    path_list = parse_selected(paths)
    data = zip_csv_from_parquets(path_list)

    if not data:
        return Response(
            content="No files to download.",
            media_type="text/plain",
            status_code=404,
        )

    return Response(
        content=data,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="bronze_files_csv.zip"'},
    )

import app_modules

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
