import { EditorView, keymap, highlightActiveLine } 
    from "https://esm.sh/@codemirror/view@6";
import { EditorState } from "https://esm.sh/@codemirror/state@6";
import { defaultKeymap } from "https://esm.sh/@codemirror/commands@6";
import { history, historyKeymap } 
    from "https://esm.sh/@codemirror/commands@6";
import { sql } from "https://esm.sh/@codemirror/lang-sql@6";
import { oneDark } from "https://esm.sh/@codemirror/theme-one-dark";
import { lineNumbers } from "https://esm.sh/@codemirror/view";

let editorView;

// Initialize editor
function createEditor(text = "") {
    editorView = new EditorView({
        state: EditorState.create({
            doc: text,
            extensions: [
                oneDark,
                sql(),
                lineNumbers(),          // ⬅ NEW
                history(),              // ⬅ NEW
                highlightActiveLine(),
                keymap.of([
                    ...defaultKeymap,
                    ...historyKeymap     // ⬅ NEW (enables Cmd-Z, Cmd-Shift-Z)
                ]),
                EditorView.lineWrapping
            ]
        }),
        parent: document.getElementById("editor")
    });
}

createEditor("");

function setEditorText(s) {
    editorView.dispatch({
        changes: { from: 0, to: editorView.state.doc.length, insert: s }
    });
}

//
// Publish functions globally:
//

window.uploadFiles = async function () {
    let fd = new FormData();
    for (let f of document.getElementById("fileUpload").files)
        fd.append("files", f);

    await fetch("/upload", { method: "POST", body: fd });

    await refreshTableList();
    alert("Uploaded!");
};


window.runLLM = async function () {
    let fd = new FormData();
    fd.append("raw", document.getElementById("raw").value);

    let res = await fetch("/run", { method: "POST", body: fd });
    let data = await res.json();

    setEditorText(data.sql);
    document.getElementById("output").innerText = data.output;

    refreshTableList();
};

window.runGolden = async function () {
    let fd = new FormData();
    fd.append("raw", document.getElementById("golden").value);

    let res = await fetch("/run", { method: "POST", body: fd });
    let data = await res.json();

    setEditorText(data.sql);
    document.getElementById("output").innerText = data.output;

    refreshTableList();
};


window.resetFS = async function () {
    if (!confirm("⚠ Delete ALL CSV files?")) return;
    await fetch("/reset", { method: "POST" });
    window.location.reload();
};

window.rerunParsed = async function () {
    let sql = editorView.state.doc.toString();

    let fd = new FormData();
    fd.append("raw", sql);

    let res = await fetch("/run", { method: "POST", body: fd });
    let data = await res.json();

    setEditorText(data.sql);
    document.getElementById("output").innerText = data.output;

    refreshTableList();
};


// Display loaded tables WITH HYPERLINKS
async function refreshTableList() {
    let res = await fetch("/tables");
    if (!res.ok) return;
    let data = await res.json();

    let ul = document.getElementById("tableListItems");
    if (!ul) return;

    ul.innerHTML = "";

    for (let item of data) {
        let li = document.createElement("li");
        li.innerHTML = `<a href="/csvview/${item.file}" target="_blank">${item.table}</a>`;

        ul.appendChild(li);
    }
}

// call on startup
document.addEventListener("DOMContentLoaded", refreshTableList);
