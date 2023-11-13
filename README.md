HEP User Training Workshop - coffea tutorial
===
### Column Object Framework For Effective Analysis ([Coffea](https://github.com/CoffeaTeam/coffea))
- Physicist friendly tools for columnar analysis.
- [uproot](https://github.com/scikit-hep/uproot5) and [awkward-array](https://github.com/scikit-hep/awkward) are used to provide an array-based syntax.

The simple template illustrates how to find Z → ee events and plot their kinematics using coffea.   
After activating the conda environment with coffea package installed, you can directly run the code by python.
```
python3 simple_coffea.py
```

Coffea Processors
---
Users can write the physics analysis in coffea.processor without concerning the technical details, then input the processor to job runner.

- Simple processor template:
    ```
    class MyProcessor(processor.ProcessorABC):
        def __init__(self):
            self._accumulator = processor.dict_accumulator({
                "sumw": processor.defaultdict_accumulator(float),
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
- The job runner chunks up jobs and parallelizes. Here the dask_executor is used.
    ```
    output = processor.run_uproot_job(
        fileset,    # fileset used in analysis
        treename,   # tree name in ROOT file
        processor_instance=Processor(), 
        executor=processor.future_executor, # Choose the suitable executor in your case.
        executor_args={
                    "client": client,
                    "schema": NanoAODSchema},
        chunksize=100000,
    )
    ```

Object and event selections
---
1. First define the objects and the related columns.  e.g. electrons 
    ```
    electrons = ak.zip({
        "pt": events.Electron_pt,
        "eta": events.Electron_eta,
        "phi": events.Electron_phi,
        "mass": events.Electron_mass,
        "charge": events.Electron_charge,
    }, with_name="PtEtaPhiMCandidate", behavior=candidate.behavior)
    ```
2. Apply the kinematic cuts to the objects.
   e.g. Select good electrons with ${p_T>15}$ and ${|\eta|<2.4}$ from all events.
    ```
    good_electrons = electrons[(electrons.pt > 15) & (abs(electrons.eta)<2.4)]
    ```
4. Apply the events selection in the similiar way.
   e.g. The events are required to have two good electrons.
    ```
    pruned_ev = events[ak.num(good_electrons) >= 2]
    ```


Output results
---
the results are shown


Materials
---
For the further studys, here are some materials for your reference.
- 
- 
