# prepContinuum
Self-contained script for initial processing of ASKAP continuum data.

Give it the SB numbers of the CAL and TARGET fields and the number of beams and channels in the observation, and it produces flagged, bandpass-calibrated 'continuum' (0.5 MHz channel) Measurement Sets for each target beam with added WEIGHT_SPECTRUM columns.

Uses CASA and ASKAPsoft mssplit. Will only run on Galaxy.
