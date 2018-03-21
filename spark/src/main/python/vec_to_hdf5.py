import glob
import h5py
import os.path
import vec_to_array

header_map = vec_to_array.header_map("/mnt/d/json/header")
n_cols = len(header_map)


for filename in glob.glob("/mnt/d/json/vector*"):

    print("processing", filename)

    mat2, mat, sex_cd, race_cd, birth_date = vec_to_array.vec_to_array(header_map, filename)

    n_rows = mat2.shape[0]

    with h5py.File(os.path.dirname(filename) + "/multi_hot_" + os.path.basename(filename) + ".hdf5", "w") as f:
        f.create_dataset("timestamps", (n_rows,), dtype='i', data=mat2)
        dset = f.create_dataset("time_series", (n_rows, n_cols), dtype='i', data=mat)
        f.attrs["sex_cd"] = sex_cd
        f.attrs["race_cd"] = race_cd
        f.attrs["birth_date"] = birth_date






