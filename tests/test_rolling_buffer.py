from scopecuisine.rolling_buffer import RollingBuffer, FillingRollingBuffer
import numpy as np


def test_rolling():
    a = RollingBuffer(5)
    a.write(np.arange(5), 1)
    np.testing.assert_equal(a.buffer, [4, 0, 1, 2, 3])
