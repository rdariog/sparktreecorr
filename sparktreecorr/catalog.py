import treecorr

from pyspark import StorageLevel

from . import spark_utils

class Catalog(object):
    
    _valid_cols = set([
        'x', 'y', 'z', 'ra', 'dec', 'r',
        'w', 'wpos', 'flag', 'g1', 'g2', 'k'
    ])
    
    _disallowed_kwargs = set(['file_name', 'num']) | _valid_cols
    
    def __init__(self, dataframe, config={}, **kwargs):
        keys = list(self._disallowed_kwargs & set(kwargs.keys()))
        if keys:
            raise ValueError("The following kwargs are not supported: {0}".format(keys))
        
        self._dataframe = dataframe
        self.config = config.copy()
        self.config.update(kwargs)
        
        # Check that every *_col specified in config or as kwarg
        # is an existing Dataframe column.
        col = {}
        for c in self._valid_cols:
            col_name = self.config.get('{}_col'.format(c), None)
            if col_name and col_name not in dataframe.columns:
                raise ValueError("{0} column does not exist".format(col_name))
            col[c] = col_name
        
        # Check that a proper combinations of columns was provided
        # and determine the coordinate system
        if col['x'] or col['y']:
            if not col['x']:
                raise AttributeError("x_col missing")
            if not col['y']:
                raise AttributeError("y_col missing")
            if col['ra']:
                raise AttributeError("ra_col not allowed in conjunction with x/y cols")
            if col['dec']:
                raise AttributeError("dec_col not allowed in conjunction with x/y cols")
            if col['r']:
                raise AttributeError("r_col not allowed in conjunction with x/y cols")
            
            if col['z']:
                self.coords = '3d'
            else:
                self.coords = 'flat'
            
        elif col['ra'] or col['dec']:
            if not col['ra']:
                raise AttributeError("ra_col missing")
            if not col['dec']:
                raise AttributeError("dec_col missing")
            if col['z']:
                raise AttributeError("z_col not allowed in conjunction with ra/dec cols")
            
            if col['r']:
                self.coords = '3d'
            else:
                self.coords = 'spherical'
            
        else:
            raise AttributeError("No valid position columns specified")
        
        self._col = col
        self._storageLevel = None
        self._cells = None
    
    def to_treecorr(self):
        config = self.config
        cols = self._col
        
        def f(df):
            return treecorr.Catalog(
                config=config,
                **{k:df[v] for k,v in cols.iteritems() if v}
            )
            
        return f
    
    def cells(self):
        if not self._cells:
            self._cells = self._dataframe.rdd.mapPartitionsWithIndex(
                spark_utils.key_by_cols(
                    self.config['hpix_col']
                )
            ).groupByKey(
            ).mapPartitionsWithIndex(
                spark_utils.to_df_chunks(grouped=True)
            )
        
        if self._storageLevel:
            self._cells.persist(self._storageLevel)
        
        return self._cells
    
    def persist(self, storageLevel=StorageLevel(False, True, False, False)):
        self._storageLevel = storageLevel
        
        if self._cells:
            self._cells.persist(self._storageLevel)
        
        return self
    
    def unpersist(self):
        self._storageLevel = None
        
        if self._cells:
            self._cells.unpersist()
        
        return self
