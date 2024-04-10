import math
import numpy as np

p2_min = 10
p2_max = 300
logstep_p2 = math.log10(p2_max / p2_min) / 7

diameters = np.zeros(8)
for i in range(8):
    diameters[i] = math.pow(10.0, math.log10(p2_min) + i * logstep_p2)


def resample_smps(
    row,
    size_cols,
):
    values = np.zeros(8)
    num_cols = len(row)
    bin_num = 0
    bin_mapping = np.zeros(8, dtype=int)

    for i in range(num_cols):
        if math.log10(float(size_cols[i])) > math.log10(diameters[bin_num]) + (
            logstep_p2 / 2.0
        ):
            bin_num += 1
        if bin_num > 7:
            bin_num = 7
        values[bin_num] += row[i]

        bin_mapping[bin_num] += 1 if bin_mapping[bin_num] else 1
        if bin_mapping[bin_num] > 13 and bin_num == 7:
            break

    values = values / bin_mapping
    values[0] = row[0]

    return values
