// simple evaluation implementation of columns and dataframe

use std::collections::HashMap;

pub enum Column {
    F(Vec<f32>),
    D(Vec<f64>),
    I(Vec<i32>),
    S(Vec<String>),
}

macro_rules! forward_op {
    ($in:ident, $op:expr) => {
        match $in {
            Column::F(x) => $op(x),
            Column::D(x) => $op(x),
            Column::I(x) => $op(x),
            Column::S(x) => $op(x),
        }
    }
}

impl Column {
    pub fn irange(l: usize) -> Column {
        let mut v: Vec<i32> = Vec::with_capacity(l);
        for i in 0..l {
            v.push(i as i32);
        }
        Column::I(v)
    }

    pub fn frange(l: usize) -> Column {
        let mut v: Vec<f32> = Vec::with_capacity(l);
        for i in 0..l {
            v.push(i as f32);
        }
        Column::F(v)
    }

    pub fn is_numeric(&self) -> bool {
        matches!(self, Column::D(_) | Column::F(_) | Column::I(_))
    }

    pub fn len(&self) -> usize {
        forward_op!(self, Vec::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Default)]
pub struct DataFrame {
    len: usize,
    columns: HashMap<String, Column>,
}

impl DataFrame {
    pub fn new() -> DataFrame {
        DataFrame{len: 0, columns: HashMap::new()}
    }

    pub fn addcol(&mut self, name: &str, data: Column) -> Result<(), &str> {
        if self.columns.len() == 0 {
            self.len = data.len();
        }
        else if self.len != data.len() {
            return Err("column length does not match dataframe length");
        }
        self.columns.insert(name.to_string(), data);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get(&self, name: &str) -> Option<&Column> {
        self.columns.get(name)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.columns.get_mut(name)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_irange() {
        let c = Column::irange(10);
        assert_eq!(c.len(), 10);
    }

    #[test]
    fn test_frange() {
        let c = Column::frange(10);
        assert_eq!(c.len(), 10);
        if let Column::F(x) = c {
            assert_eq!(x[0], 0 as f32);
        }
    }

    #[test]
    fn test_is_numeric() {
        let c = Column::frange(1);
        assert!(c.is_numeric());
    }

    #[test]
    fn test_dataframe() {
        let mut df = DataFrame::new();
        assert_eq!(df.len(), 0);
        df.addcol("mycol", Column::F(vec![1.0, 2.0, 3.0])).unwrap();
        assert_eq!(df.len(), 3);
        df.addcol("mycol2", Column::S(vec!["A".to_string(), "B".to_string(), "C".to_string()])).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_dataframe_bad_column_lens() {
        let mut df = DataFrame::new();
        df.addcol("mycol", Column::F(vec![1.0, 2.0, 3.0])).unwrap();
        df.addcol("myco2", Column::I(vec![1, 2])).unwrap();
        assert_eq!(df.len(), 3);
    }

}
