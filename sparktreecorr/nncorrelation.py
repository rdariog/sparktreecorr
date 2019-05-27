import healpy as hp
import numpy as np
import operator
import treecorr

from pyspark import Row


def hpix_ring(min_sep, max_sep, nside, nest=False):
    """\
        Return all HEALPix that may contain objects separated by an angular distance (in degrees)
        specified as [min_sep, max_sep] with respect to any object inside the HEALPix identified by hpix.
    """
    # Maximum angular distance between any pixel center and its corners
    max_pixrad = hp.max_pixrad(nside, degrees=True)
    # Why 2*
    sep_range = [np.radians(min_sep - 2 * max_pixrad), np.radians(max_sep + max_pixrad)]

    def f(hpix):
        center_vec = hp.pix2vec(nside, hpix, nest=True)

        sel_incl = hp.query_disc(nside, center_vec, sep_range[1], inclusive=True,  nest=nest)
        sel_excl = hp.query_disc(nside, center_vec, sep_range[0], inclusive=False, nest=nest)

        return np.setdiff1d(sel_incl, sel_excl, assume_unique=True)

    return f

def hpix_pairs(hpix_ring, cross=False):
    def f(i, p):
        for r in p:
            for e in hpix_ring(r.hpix):
                if not cross and e < r.hpix: # AUTO pairs must be sorted, CROSS do not
                    yield (Row(**{'hpix':e}), Row(**{'hpix':r.hpix}))
                else:
                    yield (Row(**{'hpix':r.hpix}), Row(**{'hpix':e}))
    return f

class NNCorrelation(treecorr.NNCorrelation):
    def __init__(self, config=None, **kwargs):
        treecorr.NNCorrelation.__init__(self, config, **kwargs)

        self.jk_meanr = None
        self.jk_meanlogr = None
        self.jk_weight = None
        self.jk_npairs = None

    def _pairs(self, cat, cross=False):
        if self._metric != treecorr._lib.Arc:
            raise NotImplementedError("Only Arc metric is implemented")

        min_sep = self.config['min_sep']
        max_sep = self.config['max_sep']
        hpix_nest = cat.config['hpix_nest']
        hpix_nside = cat.config['hpix_nside']

        return cat.cells().keys().mapPartitionsWithIndex(
            hpix_pairs(
                hpix_ring(min_sep, max_sep, hpix_nside, hpix_nest),
                cross,
            )
        )

    @staticmethod
    def _correlate(cat1, cat2=None, config=None):
        def f(r):
            df_a = r[1][0]
            df_b = r[1][1]

            cat_a = cat1(df_a)
            cat_b = cat2(df_b) if cat2 else cat1(df_b)

            nn = treecorr.NNCorrelation(config)

            if not cat2 and r[0][0] == r[0][1]: # Same pixel
                nn.process_auto(cat_a)
            else:
                nn.process_cross(cat_a, cat_b)

            return (r[0], nn)
        return f

    def _process(self, pairs, cells1, cells2, correlate_fn):
        if self._metric != treecorr._lib.Arc:
            raise NotImplementedError("Only Arc metric is implemented")

        return pairs.join(
            cells1
        ).map(
            lambda r: (r[1][0], (r[0], r[1][1]))
        ).join(
            cells2
        ).map(
            lambda r: ((r[1][0][0], r[0]), (r[1][0][1], r[1][1]))
        ).map(
            correlate_fn
        )

    def process_auto(self, cat, metric=None, num_threads=None):
        self._set_metric(metric, cat.coords)

        pairs = self._pairs(cat, cross=False).distinct()
        cells1 = cat.cells()
        cells2 = cat.cells()
        correlate_fn = self._correlate(cat.to_treecorr(), config=self.config)

        return self._process(pairs, cells1, cells2, correlate_fn)

    def process_cross(self, cat1, cat2, metric=None, num_threads=None):
        self._set_metric(metric, cat1.coords, cat2.coords)

        pairs = self._pairs(cat1, cross=True).distinct().intersection(
            self._pairs(cat2, cross=True).distinct())
        cells1 = cat1.cells()
        cells2 = cat2.cells()
        correlate_fn = self._correlate(cat1.to_treecorr(), cat2.to_treecorr(), config=self.config)

        return self._process(pairs, cells1, cells2, correlate_fn)

    def __iadd__(self, other):
        if not isinstance(other, NNCorrelation):
            raise AttributeError("Can only add another NNCorrelation object")

        if not (
            self.nbins == other.nbins and
            self.min_sep == other.min_sep and
            self.max_sep == other.max_sep
        ):
            raise ValueError("NNCorrelation to be added is not compatible with this one.")
