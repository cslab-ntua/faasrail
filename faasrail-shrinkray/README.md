<!--# faasrail-shrinkray-->
```text
         '||''''|                .|'''|  '||'''|,              '||`
          ||  .                  ||       ||   ||          ''   ||
          ||''|   '''|.   '''|.  `|'''|,  ||...|'  '''|.   ||   ||     ---
          ||     .|''||  .|''||   .   ||  || \\   .|''||   ||   ||
         .||.    `|..||. `|..||. ,|...|' .||  \\. `|..||. .||. .||.
   
   
         .|'''|  '||                          '||      '||'''|,
         ||       ||             ''            ||       ||   ||
         `|'''|,  ||''|, '||''|  ||  `||''|,   || //`   ||...|'  '''|.  '||  ||`
          .   ||  ||  ||  ||     ||   ||  ||   ||<<     || \\   .|''||   `|..||
         ,|...|' .||  || .||.   .||. .||  ||. .|| \\.  .||  \\. `|..||.      ||
                                                                          ,  |'
                                                                           ''
```

---

## Using the CLI tool

- Generate a specification for a 30 minutes long experiment with a maximum
request rate of 20 RPS, using FaaSRail's **"Spec"** mode (and the default
`thumbnails` mode for scaling in time):
```console
$ shrinkray/main.py -w artifacts/icy2-20231011-5.10.189__20231014175133.json -o spec_20rps_30min.csv trace --trace-dir artifacts/azure -r 20 -d 30 spec
```

- Similarly, generate a specification for a 30 minutes long experiment with a
maximum request rate of 20 RPS employing FaaSRail **"Spec"** mode, but this time
use `minute_range` as the method for scaling in time, where trace's minute 25
should be the first minute of the experiment:
```console
$ shrinkray/main.py -w artifacts/icy2-20231011-5.10.189__20231014175133.json -o spec-mr_20rps_30min.csv trace --trace-dir artifacts/azure -r 20 -d 30 spec --time-scaling minute_range -f 25
```

- Generate a specification for a 15 minutes long experiment using FaaSRail's
**"Smirnov Transform"** (or "Inverse Transform Sampling") mode with a (constant)
request rate of 10 RPS:
```console
$ shrinkray/main.py -w artifacts/icy2-20231011-5.10.189__20231014175133.json -o smirnov_10rps_15min.csv trace --trace-dir artifacts/azure -r 10 -d 15 smirnov
```

- Create a JSON list of all available workloads, formatted as expected by
FaaSCell:
```console
$ shrinkray/main.py -w artifacts/icy2-20231011-5.10.189__20231014175133.json -o faascell-functions.json functions
```
