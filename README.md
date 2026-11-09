# CRAFTS reprocessing: automated pipeline

`crafts.py` runs the whole CRAFTS pipeline for one or more pointings with as little manual work as possible.
**Your science scripts are not changed.** The thin wrappers in `steps/` call them with the same
arguments and `#SBATCH` lines as before, including the partition lines.

## Install

Copy this folder to `/aphid/temp/fast_crafts/crafts_processing/final_scripts/automation/`. Then:

```bash
cd /aphid/temp/fast_crafts/crafts_processing/final_scripts/automation
chmod +x crafts.py steps/*.sbatch
alias crafts="python3 $PWD/crafts.py -c $PWD/crafts_config.ini"
scontrol show config | grep -i MaxArraySize     # put a value <= this in max_array_size
```

Before the first real run, check the paths in `crafts_config.ini`: `[paths]` and `[pipeline]`.

## Everyday use

```bash
crafts plan  Dec+3114_06_07                  # optional: shows chunking, refuses bad epochs
crafts run   Dec+3114_06_07 Dec+2045_01_05   # submit whole pointings (all epochs), in parallel
crafts run   --all-ready --limit 5           # the next 5 clean epochs with pos files (from the audit csv)
crafts status                                # one line per epoch, all steps
crafts show  Dec+3114_06_07 --date 20220602  # detail + failed tasks + last log lines
crafts rerun-failed Dec+3114_06_07 --date 20220602 --step search [--time 00:10:00 --mem 2G]
crafts sync-cmd Dec+3114_06_07 --dest ~/crafts   # rsync command for your laptop, one folder per epoch
crafts resources                             # real time/memory per step -> suggested requests
```

Other commands:
- `run … --from STEP`: redo from a step onwards.
- `run … --only STEP`: run just one step.
- `run … --dry-run`: print the `sbatch` commands without submitting or changing anything.
- `mark-done`: accept a step by hand, with a reason.
- `cancel`: `scancel` an epoch's jobs.
- `report`: rewrite the final report.

## How it works

- **The plan.** For each epoch, `epoch.json` is built from the raw file names: 19 beams, files present
  in every beam, files per chunk and the pos file. The chunk-size logic is the same as in `audit_simple.py`.
  An epoch with missing beams, holes in the file numbering, or no pos file is **refused** unless you pass
  `--force`. If only the last file is missing in a few beams, that file number is dropped and noted.
- **One task list per step** (`pipeline/tasks/<step>.tsv`). Array task *i* runs line *i*, and the first
  column is the task id. Re-running only the failed tasks means submitting a list of just those lines
  to the same wrapper.
- **A check job after every step.** It runs once the step's jobs have ended (`afterany`). For each task it
  checks:
  1. the SLURM state (from `sacct`);
  2. that the expected output exists;
  3. that the task log doesn't contain an error pattern from `[fail_patterns]`. This catches
     `run_peasoup()`, which catches peasoup errors and carries on.

  If every task passes, the check job writes the state and **submits the next step**. If any task fails,
  it writes the failed list and the chain **stops**, with the exact `rerun-failed` command in the log.
- **No duplicate work.**
  - Update pointing writes `state/update_pointing.done`, and epochs with that marker skip step 1.
  - `run` on an epoch skips steps that are already complete.
  - An epoch that is already running is not submitted twice.
- **Fold sizing.** Fold tasks are grouped by candidate count (`[fold_classes]`), so a 30-candidate fold
  doesn't request 6 hours.
- **Array limits.** Arrays longer than `max_array_size` are split into several jobs automatically.

### What counts as "done"

| step | done when |
|---|---|
| update_pointing | log has an `Updated` line for every .fits in the epoch, no `ERROR`, no `skipping` |
| digifil | `.fil` exists and is non-empty (any half-written file is deleted before a retry) |
| clean | `_masked.fil` and `rfi_masks/<stem>_peasoup.kill` exist |
| birdies / search | exit 0, output folder exists, no peasoup/CUDA error in the log (no candidates is fine) |
| zaplist | `birdies_CCCC.zap` exists for every chunk (an empty one is created, with a warning, if a chunk had none) |
| prepare_fold | `fold_commands_<date>.sh` exists (an old one is first moved aside, because the script appends) |
| fold | `sorted.cands` exists in the command's `--outdir` |

## Files written per epoch

```
<proc_root>/<project>/<pointing>/<date>/          (same folders your scripts already use)
  01..19/, rfi_masks/, birdie_lists/, sifting_output/
  pipeline/
    epoch.json, epoch.env      the plan, and variables for the wrappers
    pipeline.log               human log: plan, submissions, OK/FAILED per step, timings
    state/<step>.json          machine state: tasks, ok, failed+reason, sacct time/mem per attempt
    state/update_pointing.done marker
    tasks/<step>.tsv           task lists (+ .failed.tsv, .rerunN.tsv)
    logs/<step>/               SLURM .out/.err per task;  logs/check_<step>_<job>.out
    final_report.txt / .json   step table (time, memory, failures), candidate funnel, file paths, rsync
<proc_root>/automation_logs/
  events.log                   one line per event, all epochs
  resources.csv                every task's elapsed time and peak memory -> `crafts resources`
```

When an epoch finishes, the final report also writes `sifting_output/all_sorted_cands.txt` and
`all_sifted_candidates.txt` (all chunks concatenated).

## Things to know

- **Check jobs submit the next step from a compute node.** Most clusters allow this. If yours doesn't, the
  chain stops after the first step and `crafts status` shows `!!`. In that case, run `crafts run <pointing>`
  from the login node to submit each next step.
- **Partition lines** are copied from your scripts. Placeholders like `${SYS_ARCH}` and `$USER` are not
  expanded inside `#SBATCH` lines. If your sbatch needs a real partition name, put it in
  `[slurm.<step>] extra = --partition=...`.
- **Your own CSV logs are no longer written.** The `tasks_<jobid>.csv` and `flock` logging from your
  original wrappers is replaced by the state files and `resources.csv`.
- **Known issues in the science scripts.** These are not changed here.
  1. `fold_pipeline_v3.py` runs every pdmp command twice (`pool.map` followed by a `for` loop).
  2. The final `rm -f … *.txt` in `fold_pipeline_v3.py` deletes `<prefix>_nearby_pulsars.txt`.
  3. `make_zap_list.py` matches `*_M01_CCCC_*` folders, so if it's re-run after the search it can pick up
     `_candidates` folders instead of `_birdies` ones. The pipeline only runs it before the search.

## Workflow

```
 audit_simple.py ──► audit_epochs.csv  (clean + pos file = ready)
        │
 crafts run <pointings…>   ──►  plan epoch.json (chunks, pos)  ──► refuse if not clean
        │
        ▼                         each box = SLURM job(s), then a CHECK job
 ┌──────────────────┐   ┌───────────────┐   ┌────────────────┐   ┌──────────────────┐
 │1 update_pointing │──►│2 digifil      │──►│3 clean         │──►│4 birdies (GPU)   │
 │  skip if marker  │   │  beam×chunk   │   │  rfifind+mask  │   │  DM 0-5 sliding  │
 └──────────────────┘   └───────────────┘   └────────────────┘   └────────┬─────────┘
                                                                           ▼
 ┌──────────────────┐   ┌───────────────┐   ┌────────────────┐   ┌──────────────────┐
 │8 fold            │◄──│7 prepare_fold │◄──│6 search (GPU)  │◄──│5 zaplist         │
 │  per beam×chunk  │   │  sift → cmds  │   │  DM 5-500 +zap │   │  .zap per chunk  │
 │  small/med/large │   └───────────────┘   └────────────────┘   └──────────────────┘
 └────────┬─────────┘
          ▼
   final_report + all_sorted_cands  ──►  crafts sync-cmd  ──►  laptop: candidate viewer

 CHECK after every box:  sacct state + outputs + log patterns
     all OK  → state/<step>.json = complete → submit next box
     any bad → state = failed, tasks/<step>.failed.tsv, chain STOPS
              → crafts rerun-failed … --step <step>  → (check) → chain continues
```
