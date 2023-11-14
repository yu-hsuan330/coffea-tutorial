HEP User Training Workshop - coffea tutorial
===
### Column Object Framework For Effective Analysis ([Coffea](https://github.com/CoffeaTeam/coffea))
- Physicist friendly tools for columnar analysis.
- [uproot](https://github.com/scikit-hep/uproot5) and [awkward-array](https://github.com/scikit-hep/awkward) are used to provide an array-based syntax.
<img src="https://coffeateam.github.io/coffea/_images/columnar.png" width="55%" style="margin:auto">

The simple template illustrates how to find Z → ee events and plot their kinematics using coffea.   
After activating the conda environment with coffea package installed, you can directly run the code by python.
```
python3 coffea_ASGC.py
```

Coffea Processors
---
Users can write the physics analysis in coffea.processor without concerning the technical details, then input the processor into job runner.

- Simple processor template:
    ```python
    class MyProcessor(processor.ProcessorABC):
        def __init__(self):
            # The accumulator stores data chunks together
            self._accumulator = processor.dict_accumulator({
                "cutflow": processor.defaultdict_accumulator(int),
                # e.g. sumw, histogram
            })

        @property
        def accumulator(self):
            return self._accumulator

        def process(self, events):
            output = self.accumulator.identity()

            # do physics analysis here ...
            return output

        def postprocess(self, accumulator):
            return accumulator
    ```
- The job runner chunks up jobs and parallelizes them. The fileset information and executor(local or distributed) should be assigned here.    
  More local executor usages can be found [here](https://coffeateam.github.io/coffea/api/coffea.processor.FuturesExecutor.html#coffea.processor.FuturesExecutor). We take the dask_executor for example. 
    ```python
    cluster = HTCondorCluster(cores=1, memory="1GB", disk="1GB")
    cluster.adapt(minimum=1, maximum=10)
    client = Client(cluster)

    output = processor.run_uproot_job(
        fileset,     # fileset used in analysis, format: {"name1":[path/to/dataset/name1], "name2":[path/to/dataset/name2]}
        treename,    # tree name in ROOT file
        processor_instance=MyProcessor(),    # Input the processor here
        executor=processor.dask_executor,    # Choose the suitable executor in your case.
        executor_args={ 
            "client": client,
            "schema": NanoAODSchema, # more options here: https://coffeateam.github.io/coffea/modules/coffea.nanoevents.html#classes
        },
    )
    ```

Usage of uproot and awkward array
---
Before starting to write the analysis code, it is good to check the file contents first by [uproot](https://github.com/scikit-hep/uproot5). CERN 2012 opendata is used in this tutorial. You can check any dataset in similar way afterward.
  + We can simply open the file in your terminal.
    ```python
    >>> import uproot
    >>> file = uproot.open('root://eospublic.cern.ch//eos/root-eos/cms_opendata_2012_nanoaod/ZZTo2e2mu.root')
    >>> file.keys() # The name of each item in the file is called a “key”.
    ['Events;1'] 
    ```
  + Accessing the objects in the tree.
    ```python
    >>> file['Events'].keys()  # All branches in the tree
    ['run', 'luminosityBlock', 'event', 'PV_npvs', ... ,'Electron_dxyErr', 'Electron_dz', 'Electron_dzErr']
    ```
    ```python
    >>> file['Events']['PV_npvs'].arrays() # values in PV_npvs branch
    <Array [{PV_npvs: 22}, ... {PV_npvs: 22}] type='1497445 * {"PV_npvs": int32}'>
    ```
Coffea relies on [awkward-array](https://github.com/scikit-hep/awkward) to build up the codes. Few features of awkward array are introduced here.
  + ak.where
    ```python
    >>> a = [2, 2, 2]
    >>> b = [-2, -2, -2]
    >>> c = [1, 0, 1]
    >>> ak.where(c, a, b)  # like: output[i] = x[i] if condition[i] else y[i]
    <Array [2, -2, 2] type='3 * int64'>
    ```
  + ak.combinations (for-loop replacement)
    ```python
    >>> import awkward as ak
    >>> array = ak.Array(["a", "b", "c", "d"])
    >>> print(ak.combinations(array, 2, axis=0))
    [('a', 'b'), ('a', 'c'), ('a', 'd'), ('b', 'c'), ('b', 'd'), ('c', 'd')]
    ```
    
    
Object and event selections
---
1. First define the objects and the related columns.  e.g. electrons 
    ```python
    electrons = ak.zip({
        "pt": events.Electron.pt,
        "eta": events.Electron.eta,
        "phi": events.Electron.phi,
        "mass": events.Electron.mass,
        "charge": events.Electron.charge,
    }, with_name="PtEtaPhiMCandidate", behavior=candidate.behavior)
    ```
2. Apply the kinematic cuts to the objects.
   e.g. Select good electrons with ${p_T>15}$ and ${|\eta|<2.4}$ from all events.
    ```python
    good_electrons = electrons[(electrons.pt > 15) & (abs(electrons.eta) < 2.4)]
    ```
3. Apply the events selection in the similiar way.
   e.g. The events are required to have two good electrons.
    ```python
    pruned_ev = events[ak.num(good_electrons) >= 2]
    ```

Plot the histogram
---
The results can be stored as histograms.
e.g. Plot the Z mass distribution in **coffea_ASGC.py**.
```python
# Bins and categories of the histogram are defined here.
dataset_axis = hist.axis.StrCategory(name="dataset", label="", categories=[], growth=True)
mZ_axis = hist.axis.Regular(name="mZ", label="Z mass [GeV]", bins=30, start=60, stop=120)
h = hist.Hist(dataset_axis, mZ_axis)

# Fill the histogram
h.fill(dataset=dataset, mZ=pruned_Z.mass)
```

Materials
---
For further study, here are some materials for your reference.
- [uproot tutorial](https://masonproffitt.github.io/uproot-tutorial/)
- [coffea introduction talk](https://indico.cern.ch/event/833895/contributions/3577894/attachments/1928017/3192492/PyHEP_LindseyGray_17102019.pdf)
- coffea mattermost
