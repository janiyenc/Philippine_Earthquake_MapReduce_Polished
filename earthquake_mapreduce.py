"""Philippine earthquake MapReduce Studio. Standard-library only."""
import csv
import math
import os
import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from concurrent.futures import ProcessPoolExecutor
import threading
import queue
import multiprocessing
from collections import defaultdict
from datetime import datetime
from pathlib import Path

DEFAULT_CSV = Path(__file__).with_name('earthquakes_1900-2025_cleaned.csv')
MODES = ('Region', 'Year', 'Magnitude band', 'Source')

def parse_year(row):
    # ISO dates and common day/month/year formats; the source's date column is DD/MM/YYYY.
    s = row.get('date', '').strip()
    if s:
        for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%m/%d/%Y'):
            try:
                return str(datetime.strptime(s, fmt).year)
            except ValueError:
                pass
    return 'Unknown'

def group_key(row, mode):
    if mode == 'Region':
        return row.get('region', '').strip() or 'Unknown'
    if mode == 'Year':
        return parse_year(row)
    if mode == 'Source':
        return row.get('source', '').strip() or 'Unknown'
    try:
        m = float(row['magnitude'])
        if not math.isfinite(m):
            return 'Unknown'
        return '< 3' if m < 3 else '3–3.9' if m < 4 else '4–4.9' if m < 5 else '5–5.9' if m < 6 else '6–6.9' if m < 7 else '7+'
    except (ValueError, TypeError, KeyError):
        return 'Unknown'

def mapper(rows, mode):
    result = defaultdict(lambda: [0, 0.0, 0, 0.0, 0])
    for row in rows:
        key = group_key(row, mode)
        a = result[key]
        a[0] += 1
        for name, sum_idx, count_idx in [('magnitude', 1, 2), ('depth', 3, 4)]:
            try:
                v = float(row.get(name, ''))
                if math.isfinite(v):
                    a[sum_idx] += v
                    a[count_idx] += 1
            except (ValueError, TypeError):
                pass
    return dict(result)

def shuffle_reduce(partials):
    out = defaultdict(lambda: [0, 0.0, 0, 0.0, 0])
    for part in partials:
        for key, vals in part.items():
            target = out[key]
            for i, value in enumerate(vals):
                target[i] += value
    return dict(out)

def read_csv(path):
    with open(path, encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        required = {'date', 'region', 'magnitude', 'depth', 'source'}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError('Missing required columns: ' + ', '.join(sorted(missing)))
        return list(reader)

def run_comparison(rows, mode, workers):
    t = time.perf_counter()
    serial = shuffle_reduce([mapper(rows, mode)])
    serial_time = time.perf_counter() - t
    chunks = [rows[i::workers] for i in range(workers)]
    t = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        partials = list(pool.map(mapper, chunks, [mode] * workers))
    parallel_time = time.perf_counter() - t
    parallel = shuffle_reduce(partials)
    match = serial.keys() == parallel.keys() and all(
        serial[k][0] == parallel[k][0] and serial[k][2] == parallel[k][2]
        and serial[k][4] == parallel[k][4]
        and math.isclose(serial[k][1], parallel[k][1], abs_tol=1e-5, rel_tol=1e-10)
        and math.isclose(serial[k][3], parallel[k][3], abs_tol=1e-5, rel_tol=1e-10)
        for k in serial
    )
    return {'serial': serial_time, 'parallel': parallel_time, 'result': parallel, 'match': match,
            'chunk_sizes': [len(c) for c in chunks], 'partials': partials}


def timed_mapper(payload):
    index, rows, mode = payload
    started = time.perf_counter()
    mapped = mapper(rows, mode)
    elapsed = time.perf_counter() - started
    return {'worker': index, 'pid': os.getpid(), 'rows': len(rows), 'groups': len(mapped),
            'seconds': elapsed, 'mapped': mapped}

def execute_parallel(rows, mode, workers):
    # Chunk construction is included in elapsed time; loading the CSV is not.
    started = time.perf_counter()
    chunks = [rows[i::workers] for i in range(workers)]
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context('spawn')) as pool:
        details = list(pool.map(timed_mapper, [(i + 1, chunk, mode) for i, chunk in enumerate(chunks)]))
    merged = shuffle_reduce([d['mapped'] for d in details])
    elapsed = time.perf_counter() - started
    return merged, elapsed, details

def verify_results(a, b):
    return a.keys() == b.keys() and all(
        a[k][0] == b[k][0] and a[k][2] == b[k][2] and a[k][4] == b[k][4]
        and math.isclose(a[k][1], b[k][1], abs_tol=1e-5, rel_tol=1e-10)
        and math.isclose(a[k][3], b[k][3], abs_tol=1e-5, rel_tol=1e-10)
        for k in a)

def benchmark_all(rows, mode, worker_counts=(1, 2, 4, 8)):
    start = time.perf_counter()
    baseline = shuffle_reduce([mapper(rows, mode)])
    serial = time.perf_counter() - start
    samples = []
    for n in worker_counts:
        result, seconds, details = execute_parallel(rows, mode, n)
        samples.append({'workers': n, 'seconds': seconds, 'speedup': serial / seconds if seconds else 0,
                        'match': verify_results(baseline, result), 'details': details, 'groups': len(result)})
    return serial, samples, baseline

class App:
    """Presentation-friendly UI. Processing functions above remain unchanged."""
    NAVY = '#112d48'
    BG = '#f3f6fa'
    WHITE = '#ffffff'
    INK = '#183047'
    MUTED = '#62758a'
    TEAL = '#087f79'
    BORDER = '#dce5ed'

    def __init__(self, root):
        self.root = root
        root.title('Philippine Earthquake MapReduce | Group 4')
        root.geometry('1250x820')
        root.minsize(900, 620)
        root.configure(bg=self.BG)
        self.path = tk.StringVar(value=str(DEFAULT_CSV))
        self.mode = tk.StringVar(value='Region')
        self.workers = tk.IntVar(value=4)
        self.status = tk.StringVar(value='Ready — load a dataset, then run the analysis.')
        self.rows = None
        self.results = None
        self.benchmarks = []
        self.benchmark_serial = None
        self.ui_queue = queue.Queue()
        self.busy = False
        self.configure_style()
        self.make_ui()
        self.root.after(120, self.poll_queue)
        if DEFAULT_CSV.exists():
            self.load()

    def configure_style(self):
        s = ttk.Style(self.root)
        if 'clam' in s.theme_names():
            s.theme_use('clam')
        s.configure('TNotebook', background=self.BG, borderwidth=0)
        s.configure('TNotebook.Tab', font=('Segoe UI', 11, 'bold'), padding=(20, 11), background='#e5ebf1', foreground=self.INK)
        s.map('TNotebook.Tab', background=[('selected', self.WHITE)], foreground=[('selected', self.TEAL)])
        s.configure('TButton', font=('Segoe UI', 10), padding=(12, 8))
        s.configure('Accent.TButton', font=('Segoe UI', 10, 'bold'), padding=(14, 9), background=self.TEAL, foreground='white')
        s.map('Accent.TButton', background=[('active', '#086b66'), ('disabled', '#9ab8b5')])
        s.configure('Treeview', font=('Segoe UI', 10), rowheight=33, background='white', fieldbackground='white', foreground=self.INK)
        s.configure('Treeview.Heading', font=('Segoe UI', 10, 'bold'), padding=(9, 10), background='#eaf0f5', foreground=self.INK)
        s.configure('TCombobox', padding=5)

    def label(self, parent, text, size=10, bold=False, color=None, bg=None, **kw):
        return tk.Label(parent, text=text, font=('Segoe UI', size, 'bold' if bold else 'normal'),
                        fg=color or self.INK, bg=bg or parent.cget('bg'), **kw)

    def panel(self, parent, title=None, subtitle=None):
        outer = tk.Frame(parent, bg=self.WHITE, highlightbackground=self.BORDER, highlightthickness=1)
        outer.pack(fill='x', padx=18, pady=(0, 14))
        if title:
            self.label(outer, title, 13, True).pack(anchor='w', padx=20, pady=(16, 2))
        if subtitle:
            self.label(outer, subtitle, 9, color=self.MUTED, wraplength=1000, justify='left').pack(anchor='w', padx=20, pady=(0, 12))
        return outer

    def make_ui(self):
        header = tk.Frame(self.root, bg=self.NAVY, padx=26, pady=18)
        header.pack(fill='x')
        self.label(header, 'PHILIPPINE EARTHQUAKE  /  MAPREDUCE STUDIO', 20, True, 'white').pack(anchor='w')
        self.label(header, 'GROUP 4  •  Parallel data processing  •  Philippine earthquake records', 10,
                   color='#bed6e7').pack(anchor='w', pady=(4, 0))

        controls = tk.Frame(self.root, bg=self.WHITE, padx=22, pady=14, highlightbackground=self.BORDER, highlightthickness=1)
        controls.pack(fill='x', padx=18, pady=(15, 7))
        row1 = tk.Frame(controls, bg=self.WHITE)
        row1.pack(fill='x')
        self.label(row1, 'DATASET', 9, True, self.MUTED).pack(side='left', padx=(0, 10))
        ttk.Button(row1, text='Open CSV', command=self.pick).pack(side='left')
        self.path_label = self.label(row1, '', 9, color=self.MUTED, anchor='w')
        self.path_label.pack(side='left', fill='x', expand=True, padx=12)
        self.path.trace_add('write', lambda *_: self.path_label.configure(text=self.path.get()))
        self.path_label.configure(text=self.path.get())
        row2 = tk.Frame(controls, bg=self.WHITE)
        row2.pack(fill='x', pady=(13, 0))
        self.label(row2, 'GROUP BY', 9, True, self.MUTED).pack(side='left', padx=(0, 8))
        ttk.Combobox(row2, textvariable=self.mode, values=MODES, state='readonly', width=18).pack(side='left', padx=(0, 18))
        self.label(row2, 'WORKERS', 9, True, self.MUTED).pack(side='left', padx=(0, 8))
        ttk.Spinbox(row2, from_=1, to=16, textvariable=self.workers, width=5).pack(side='left', padx=(0, 18))
        self.runbtn = ttk.Button(row2, text='Run & Compare', style='Accent.TButton', command=self.run)
        self.runbtn.pack(side='left', padx=(0, 8))
        self.benchbtn = ttk.Button(row2, text='Benchmark 1 / 2 / 4 / 8', command=self.start_benchmark)
        self.benchbtn.pack(side='left', padx=(0, 8))
        ttk.Button(row2, text='Export CSV', command=self.export).pack(side='left')

        statusbar = tk.Frame(self.root, bg=self.BG)
        statusbar.pack(fill='x', padx=22, pady=(2, 8))
        self.label(statusbar, '●', 12, True, self.TEAL).pack(side='left')
        tk.Label(statusbar, textvariable=self.status, bg=self.BG, fg=self.MUTED,
                 font=('Segoe UI', 10), anchor='w').pack(side='left', padx=7, fill='x')

        self.tabs = ttk.Notebook(self.root)
        self.tabs.pack(fill='both', expand=True, padx=18, pady=(0, 16))
        self.overview = self.scroll_tab('Overview')
        self.benchmark_tab = self.scroll_tab('Performance benchmark')
        self.workers_tab = self.scroll_tab('Worker details')
        self.results_tab = self.scroll_tab('Reduced results')
        self.guide_tab = self.scroll_tab('How it works')
        self.build_overview()
        self.build_benchmark()
        self.build_workers()
        self.build_results()
        self.build_guide()

    def scroll_tab(self, name):
        host = tk.Frame(self.tabs, bg=self.BG)
        self.tabs.add(host, text=name)
        canvas = tk.Canvas(host, bg=self.BG, highlightthickness=0)
        scrollbar = ttk.Scrollbar(host, orient='vertical', command=canvas.yview)
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        canvas.pack(side='left', fill='both', expand=True)
        content = tk.Frame(canvas, bg=self.BG)
        wid = canvas.create_window((0, 0), window=content, anchor='nw')
        content.bind('<Configure>', lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.bind('<Configure>', lambda e: canvas.itemconfigure(wid, width=e.width))
        def wheel(e):
            if getattr(e, 'delta', 0):
                canvas.yview_scroll(-int(e.delta / 120) or (-1 if e.delta > 0 else 1), 'units')
            elif getattr(e, 'num', 0) in (4, 5):
                canvas.yview_scroll(-1 if e.num == 4 else 1, 'units')
            return 'break'
        def enter(e):
            canvas.bind_all('<MouseWheel>', wheel)
            canvas.bind_all('<Button-4>', wheel)
            canvas.bind_all('<Button-5>', wheel)
        def leave(e):
            canvas.unbind_all('<MouseWheel>')
            canvas.unbind_all('<Button-4>')
            canvas.unbind_all('<Button-5>')
        # Bind on the page and its children, but leave Treeview scrolling native.
        def bind_recursive(widget):
            if isinstance(widget, (ttk.Treeview, ttk.Scrollbar)):
                return
            widget.bind('<Enter>', enter, add='+')
            widget.bind('<Leave>', leave, add='+')
            for child in widget.winfo_children():
                bind_recursive(child)
        content.after(200, lambda: bind_recursive(content))
        return content

    def metric(self, parent, title, variable):
        frame = tk.Frame(parent, bg='#f5f8fb', padx=15, pady=12)
        frame.pack(side='left', fill='both', expand=True, padx=5)
        self.label(frame, title.upper(), 9, True, self.MUTED, '#f5f8fb').pack(anchor='w')
        tk.Label(frame, textvariable=variable, font=('Segoe UI', 19, 'bold'),
                 bg='#f5f8fb', fg=self.NAVY).pack(anchor='w', pady=(4, 0))

    def build_overview(self):
        self.label(self.overview, 'Analysis overview', 19, True, self.NAVY, self.BG).pack(anchor='w', padx=20, pady=(20, 13))
        p = self.panel(self.overview, 'Dataset and processing summary', 'Live results from the selected grouping and worker count.')
        r = tk.Frame(p, bg=self.WHITE)
        r.pack(fill='x', padx=14, pady=(0, 18))
        self.records_text = tk.StringVar(value='—')
        self.groups_text = tk.StringVar(value='—')
        self.serial_text = tk.StringVar(value='—')
        self.parallel_text = tk.StringVar(value='—')
        for title, var in [('Records', self.records_text), ('Groups', self.groups_text),
                           ('Sequential', self.serial_text), ('Parallel', self.parallel_text)]:
            self.metric(r, title, var)
        self.validation_text = tk.StringVar(value='Run the analysis to verify sequential and parallel results.')
        tk.Label(p, textvariable=self.validation_text, bg=self.WHITE, fg=self.TEAL,
                 font=('Segoe UI', 10, 'bold')).pack(anchor='w', padx=20, pady=(0, 16))

        p = self.panel(self.overview, 'The MapReduce pipeline',
                       'Each stage corresponds to a real operation in the processing engine; this diagram is a summary, not a live animation.')
        self.diagram = tk.Canvas(p, bg=self.WHITE, height=160, highlightthickness=0)
        self.diagram.pack(fill='x', padx=14, pady=(0, 16))
        self.diagram.bind('<Configure>', lambda e: self.draw_diagram())

        p = self.panel(self.overview, 'Top 10 groups by earthquake count',
                       'Bar length represents the number of earthquake records in each group, not earthquake risk.')
        self.chart = tk.Canvas(p, bg=self.WHITE, height=340, highlightthickness=0)
        self.chart.pack(fill='x', padx=16, pady=(0, 18))
        self.chart.bind('<Configure>', lambda e: self.draw_chart())
        self.draw_diagram()

    def build_benchmark(self):
        self.label(self.benchmark_tab, 'Performance benchmark', 19, True, self.NAVY, self.BG).pack(anchor='w', padx=20, pady=(20, 6))
        self.label(self.benchmark_tab, 'Compare sequential processing against 1, 2, 4, and 8 worker processes. Lower execution time is better.',
                   10, color=self.MUTED, bg=self.BG, wraplength=900).pack(anchor='w', padx=20, pady=(0, 16))
        p = self.panel(self.benchmark_tab, 'Execution time comparison',
                       'Elapsed wall-clock time includes worker startup, inter-process communication, and result merging.')
        self.bench_canvas = tk.Canvas(p, bg=self.WHITE, height=300, highlightthickness=0)
        self.bench_canvas.pack(fill='x', padx=15, pady=(0, 15))
        self.bench_canvas.bind('<Configure>', lambda e: self.draw_benchmark())
        p = self.panel(self.benchmark_tab, 'Measured configurations',
                       'Speedup = sequential time ÷ parallel time. A value below 1.0× means the parallel run was slower.')
        self.benchmark_table = self.table(p, [('workers', 'Configuration', 180), ('time', 'Time (seconds)', 180),
                                              ('speedup', 'Speedup', 140), ('valid', 'Verification', 160)], height=6)
        self.benchmark_note = tk.StringVar(value='Click “Benchmark 1 / 2 / 4 / 8” above to populate this comparison.')
        tk.Label(p, textvariable=self.benchmark_note, bg=self.WHITE, fg=self.MUTED,
                 font=('Segoe UI', 10), wraplength=1000, justify='left').pack(anchor='w', padx=20, pady=(0, 18))

    def build_workers(self):
        self.label(self.workers_tab, 'Worker contributions', 19, True, self.NAVY, self.BG).pack(anchor='w', padx=20, pady=(20, 6))
        self.label(self.workers_tab, 'Inspect actual worker task assignments and process IDs. A process may handle more than one chunk.',
                   10, color=self.MUTED, bg=self.BG).pack(anchor='w', padx=20, pady=(0, 16))
        p = self.panel(self.workers_tab, 'Worker execution details',
                       'Every row represents a completed chunk task. PID identifies the operating-system process that executed it.')
        self.worker_table = self.table(p, [('config', 'Configuration', 150), ('worker', 'Task #', 90),
                                           ('pid', 'Process ID', 130), ('records', 'Records handled', 180),
                                           ('seconds', 'Map time (s)', 140), ('keys', 'Partial groups', 150)], height=15)
        p = self.panel(self.workers_tab, 'Understanding the phases')
        steps = [
            ('01  INPUT', 'Load the earthquake CSV into memory. Each record contains date, region, magnitude, depth, and source.'),
            ('02  MAP', 'Split records into chunks. Each process computes local counts and sums for its assigned group keys.'),
            ('03  SHUFFLE', 'Collect partial aggregates from worker processes and combine entries sharing the same group key.'),
            ('04  REDUCE', 'Produce final earthquake counts and average magnitude and depth for each group.'),
            ('05  VALIDATE', 'Compare the parallel result with the sequential baseline, allowing for floating-point tolerance.')]
        for title, desc in steps:
            line = tk.Frame(p, bg=self.WHITE)
            line.pack(fill='x', padx=20, pady=9)
            self.label(line, title, 10, True, self.TEAL).pack(anchor='w')
            self.label(line, desc, 10, color=self.MUTED, wraplength=940, justify='left').pack(anchor='w', pady=(4, 0))
        tk.Frame(p, bg=self.WHITE, height=12).pack()

    def build_results(self):
        self.label(self.results_tab, 'Reduced results', 19, True, self.NAVY, self.BG).pack(anchor='w', padx=20, pady=(20, 6))
        self.label(self.results_tab, 'Grouped earthquake statistics. Select another Group by value and run again to change the results.',
                   10, color=self.MUTED, bg=self.BG).pack(anchor='w', padx=20, pady=(0, 16))
        p = self.panel(self.results_tab, 'Aggregated data',
                       'Average magnitude and average depth use available numeric values; missing measurements are excluded from those averages.')
        self.tree = self.table(p, [('group', 'Group', 230), ('count', 'Earthquake count', 185),
                                   ('meanmag', 'Average magnitude', 195), ('meandepth', 'Average depth (km)', 195)], height=17)
        self.result_note = tk.StringVar(value='Run & Compare to display grouped results.')
        tk.Label(p, textvariable=self.result_note, bg=self.WHITE, fg=self.MUTED,
                 font=('Segoe UI', 10)).pack(anchor='w', padx=20, pady=(0, 18))

    def build_guide(self):
        self.label(self.guide_tab, 'Project guide', 19, True, self.NAVY, self.BG).pack(anchor='w', padx=20, pady=(20, 14))
        p = self.panel(self.guide_tab, 'How to demonstrate this project')
        for title, desc in [
            ('1. Load a CSV', 'Use the included Philippine earthquake dataset or another CSV with date, region, magnitude, depth, and source columns.'),
            ('2. Choose a grouping', 'Region, Year, Magnitude band, or Source. This determines the key used by the mapper.'),
            ('3. Run & Compare', 'Process the same dataset sequentially and with the selected number of processes. Verify that results match.'),
            ('4. Run the benchmark', 'Test 1, 2, 4, and 8 processes. Compare wall-clock time and correctness across configurations.'),
            ('5. Explore the outputs', 'Use Worker details for process IDs, Overview for visual results, and Reduced results for the final aggregates.'),
            ('6. Export the results', 'Save the latest Run & Compare output to a CSV file for documentation.')]:
            line = tk.Frame(p, bg=self.WHITE)
            line.pack(fill='x', padx=20, pady=10)
            self.label(line, title, 11, True, self.TEAL).pack(anchor='w')
            self.label(line, desc, 10, color=self.MUTED, wraplength=940, justify='left').pack(anchor='w', pady=(4, 0))
        tk.Frame(p, bg=self.WHITE, height=12).pack()
        p = self.panel(self.guide_tab, 'Technical notes')
        for note in [
            'This is MapReduce-style multiprocessing on one computer; it is not a distributed Hadoop or Spark cluster.',
            'The dataset is community-maintained. Regional counts represent records in this dataset, not a complete national earthquake census.',
            'Parallel execution is not guaranteed to be faster. Process startup and data transfer may outweigh the benefits for lightweight aggregations.',
            'CSV loading is excluded from the benchmark timing. Performance numbers vary between runs and computers.'
        ]:
            self.label(p, '• ' + note, 10, color=self.MUTED, wraplength=940, justify='left').pack(anchor='w', padx=20, pady=8)
        tk.Frame(p, bg=self.WHITE, height=12).pack()

    def table(self, parent, columns, height=10):
        frame = tk.Frame(parent, bg=self.WHITE)
        frame.pack(fill='x', padx=18, pady=(2, 14))
        names = [x[0] for x in columns]
        tree = ttk.Treeview(frame, columns=names, show='headings', height=height)
        for key, title, width in columns:
            tree.heading(key, text=title)
            tree.column(key, width=width, minwidth=90, anchor='center', stretch=True)
        tree.tag_configure('odd', background='#f5f8fb')
        sb = ttk.Scrollbar(frame, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        tree.pack(side='left', fill='x', expand=True)
        sb.pack(side='right', fill='y')
        return tree

    def set_busy(self, busy, message=None):
        self.busy = busy
        self.runbtn.configure(state='disabled' if busy else 'normal')
        self.benchbtn.configure(state='disabled' if busy else 'normal')
        if message:
            self.status.set(message)

    def pick(self):
        if self.busy:
            return
        path = filedialog.askopenfilename(filetypes=[('CSV files', '*.csv')])
        if path:
            self.path.set(path)
            self.load()

    def load(self):
        try:
            self.rows = read_csv(self.path.get())
            self.results = None
            self.benchmarks = []
            self.benchmark_serial = None
            self.records_text.set(f'{len(self.rows):,}')
            self.groups_text.set('—')
            self.serial_text.set('—')
            self.parallel_text.set('—')
            self.validation_text.set('Ready for processing')
            for tree in (self.tree, self.benchmark_table, self.worker_table):
                tree.delete(*tree.get_children())
            self.draw_chart()
            self.draw_benchmark()
            self.draw_diagram()
            self.status.set(f'Loaded {len(self.rows):,} records. Choose a grouping and run the analysis.')
        except Exception as exc:
            self.rows = None
            messagebox.showerror('CSV error', str(exc))

    def run(self):
        if self.busy:
            return
        if self.rows is None:
            self.load()
        if self.rows is None:
            return
        try:
            n = int(self.workers.get())
            if not 1 <= n <= 16:
                raise ValueError('Worker count must be between 1 and 16.')
        except (ValueError, tk.TclError) as exc:
            messagebox.showerror('Invalid workers', str(exc))
            return
        self.set_busy(True, 'Processing earthquake records. Please wait…')
        rows, mode = self.rows, self.mode.get()
        def work():
            try:
                self.ui_queue.put(('run_success', (run_comparison(rows, mode, n), n)))
            except Exception as exc:
                self.ui_queue.put(('run_error', str(exc)))
        threading.Thread(target=work, daemon=True).start()

    def start_benchmark(self):
        if self.busy:
            return
        if self.rows is None:
            messagebox.showerror('No data', 'Load a valid CSV first.')
            return
        self.set_busy(True, 'Benchmark running: testing 1, 2, 4, and 8 workers…')
        rows, mode = self.rows, self.mode.get()
        def work():
            try:
                self.ui_queue.put(('bench_success', benchmark_all(rows, mode)))
            except Exception as exc:
                self.ui_queue.put(('bench_error', str(exc)))
        threading.Thread(target=work, daemon=True).start()

    def poll_queue(self):
        try:
            while True:
                kind, payload = self.ui_queue.get_nowait()
                self.set_busy(False)
                if kind.endswith('error'):
                    self.status.set('Operation failed; see error message.')
                    messagebox.showerror('Processing error', payload)
                elif kind == 'run_success':
                    self.apply_run(*payload)
                elif kind == 'bench_success':
                    self.apply_benchmark(*payload)
        except queue.Empty:
            pass
        self.root.after(120, self.poll_queue)

    def apply_run(self, result, n):
        self.results = result
        data = result['result']
        self.records_text.set(f'{sum(v[0] for v in data.values()):,}')
        self.groups_text.set(str(len(data)))
        self.serial_text.set(f"{result['serial']:.4f} s")
        self.parallel_text.set(f"{result['parallel']:.4f} s")
        self.validation_text.set('✓  Sequential and parallel results MATCH' if result['match'] else '⚠  Results MISMATCH — investigate')
        self.tree.delete(*self.tree.get_children())
        for i, (key, v) in enumerate(sorted(data.items(), key=lambda x: (-x[1][0], x[0]))):
            self.tree.insert('', 'end', values=(key, f'{v[0]:,}', f'{v[1]/v[2]:.3f}' if v[2] else 'N/A',
                                                  f'{v[3]/v[4]:.2f}' if v[4] else 'N/A'), tags=('odd',) if i % 2 else ())
        self.result_note.set(f'{len(data):,} groups  •  Grouping: {self.mode.get()}  •  {n} worker processes')
        self.draw_chart()
        self.draw_diagram()
        self.status.set(f'Processing complete: {len(data):,} groups, {n} workers. Verification: {"MATCH" if result["match"] else "MISMATCH"}.')

    def apply_benchmark(self, serial, samples, baseline):
        self.benchmark_serial = serial
        self.benchmarks = samples
        self.benchmark_table.delete(*self.benchmark_table.get_children())
        entries = [('Sequential', serial, '1.000×', 'BASELINE')]
        entries += [(f"{x['workers']} workers", x['seconds'], f"{x['speedup']:.3f}×",
                     'MATCH' if x['match'] else 'MISMATCH') for x in samples]
        for i, (name, seconds, speed, valid) in enumerate(entries):
            self.benchmark_table.insert('', 'end', values=(name, f'{seconds:.4f}', speed, valid),
                                        tags=('odd',) if i % 2 else ())
        self.worker_table.delete(*self.worker_table.get_children())
        i = 0
        for sample in samples:
            for detail in sample['details']:
                self.worker_table.insert('', 'end', values=(f"{sample['workers']} workers", detail['worker'],
                    detail['pid'], f"{detail['rows']:,}", f"{detail['seconds']:.4f}", detail['groups']),
                    tags=('odd',) if i % 2 else ())
                i += 1
        fastest = min(samples, key=lambda x: x['seconds'])
        self.benchmark_note.set(f"Fastest parallel configuration in this run: {fastest['workers']} workers "
                                f"({fastest['seconds']:.4f} s). All outputs match: "
                                f"{'YES' if all(x['match'] for x in samples) else 'NO'}.")
        self.draw_benchmark()
        self.status.set('Benchmark complete. See Performance benchmark and Worker details tabs.')
        self.tabs.select(1)

    def draw_diagram(self):
        if not hasattr(self, 'diagram'):
            return
        c = self.diagram
        c.delete('all')
        w = max(c.winfo_width(), 820)
        labels = [('INPUT', f'{len(self.rows):,} records' if self.rows is not None else 'CSV records'),
                  ('MAP', f"{len(self.results['partials'])} chunks" if self.results else 'Worker chunks'),
                  ('SHUFFLE', 'Combine group keys'),
                  ('REDUCE', f"{len(self.results['result'])} groups" if self.results else 'Final aggregates')]
        margin, gap = 24, 44
        bw = max(130, (w - 2 * margin - 3 * gap) / 4)
        colors = ['#e2edfc', '#d5f5ee', '#fff0c7', '#eae3ff']
        for i, (title, subtitle) in enumerate(labels):
            x = margin + i * (bw + gap)
            c.create_rectangle(x, 20, x + bw, 122, fill=colors[i], outline=self.BORDER)
            c.create_text(x + bw/2, 52, text=title, font=('Segoe UI', 14, 'bold'), fill=self.NAVY)
            c.create_text(x + bw/2, 87, text=subtitle, font=('Segoe UI', 10), fill=self.MUTED)
            if i < 3:
                c.create_text(x + bw + gap/2, 70, text='→', font=('Segoe UI', 19, 'bold'), fill=self.MUTED)

    def draw_chart(self):
        if not hasattr(self, 'chart'):
            return
        c = self.chart
        c.delete('all')
        if not self.results:
            c.create_text(30, 38, anchor='w', text='Run & Compare to view the top earthquake groups.',
                          fill=self.MUTED, font=('Segoe UI', 11))
            return
        items = sorted(self.results['result'].items(), key=lambda x: -x[1][0])[:10]
        if not items:
            return
        w = max(c.winfo_width(), 800)
        maxval = max(v[0] for _, v in items) or 1
        start = 205
        available = max(100, w - start - 115)
        for i, (name, v) in enumerate(items):
            y = 15 + i * 31
            display = name if len(name) < 27 else name[:24] + '…'
            c.create_text(12, y + 10, anchor='w', text=display, font=('Segoe UI', 10), fill=self.INK)
            bw = available * v[0] / maxval
            c.create_rectangle(start, y, start + bw, y + 21, fill=self.TEAL, outline='')
            c.create_text(start + bw + 8, y + 10, anchor='w', text=f'{v[0]:,}',
                          font=('Segoe UI', 10, 'bold'), fill=self.INK)

    def draw_benchmark(self):
        if not hasattr(self, 'bench_canvas'):
            return
        c = self.bench_canvas
        c.delete('all')
        if not self.benchmarks:
            c.create_text(25, 35, anchor='w', text='No benchmark yet. Click the Benchmark button to compare configurations.',
                          fill=self.MUTED, font=('Segoe UI', 11))
            return
        w = max(c.winfo_width(), 800)
        entries = [('Sequential', self.benchmark_serial)] + [(f"{x['workers']} workers", x['seconds']) for x in self.benchmarks]
        maxval = max(t for _, t in entries) or 1
        start, available = 150, max(100, w - 300)
        for i, (name, seconds) in enumerate(entries):
            y = 18 + i * 53
            c.create_text(16, y + 15, text=name, anchor='w', font=('Segoe UI', 11, 'bold'), fill=self.INK)
            bw = available * seconds / maxval
            c.create_rectangle(start, y, start + bw, y + 29, fill=self.TEAL if i else '#71849a', outline='')
            c.create_text(start + bw + 10, y + 15, text=f'{seconds:.4f} s', anchor='w',
                          font=('Segoe UI', 10, 'bold'), fill=self.INK)

    def export(self):
        if not self.results:
            messagebox.showinfo('No results', 'Run & Compare first to generate results.')
            return
        path = filedialog.asksaveasfilename(defaultextension='.csv', filetypes=[('CSV files', '*.csv')])
        if not path:
            return
        try:
            with open(path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['group', 'earthquake_count', 'average_magnitude', 'average_depth_km'])
                for key, v in sorted(self.results['result'].items(), key=lambda x: -x[1][0]):
                    writer.writerow([key, v[0], v[1]/v[2] if v[2] else '', v[3]/v[4] if v[4] else ''])
            messagebox.showinfo('Export complete', f'Saved to:\n{path}')
        except OSError as exc:
            messagebox.showerror('Export failed', str(exc))


if __name__ == '__main__':
    multiprocessing.freeze_support()
    root = tk.Tk()
    App(root)
    root.mainloop()
