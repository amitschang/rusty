use std::{any::Any, collections::HashMap, slice::Iter, sync::Arc};

pub trait Column {
    fn as_any(&self) -> &dyn Any;
    fn len(&self) -> usize;
}

pub struct Col<T: 'static>(Vec<T>);

impl<T> Col<T> {
    pub fn new() -> Col<T> {
        Col(Vec::<T>::new())
    }

    pub fn from_vec(v: Vec<T>) -> Col<T> {
        Col(v)
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.0.iter()
    }
}

impl<T> Column for Col<T> {
    fn len(&self) -> usize {
        self.0.len()
    }
    
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub trait AsVec {
    fn as_vec<T: 'static>(&self) -> &Vec<T>;
    fn as_col<T>(&self) -> &Col<T>;
}

impl AsVec for dyn Column {
    fn as_vec<T: 'static>(&self) -> &Vec<T> {
        &self.as_any().downcast_ref::<Col<T>>().expect("cannot convert to vec of type").0
    }

    fn as_col<T>(&self) -> &Col<T> {
        self.as_any().downcast_ref::<Col<T>>().expect("cannot convert to col of specified type")
    }
}

type ColRef = Arc<dyn Column>;
type DFCols = HashMap<String, ColRef>;
pub struct DataFrame {
    len: usize,
    columns: DFCols,
}

impl DataFrame {
    pub fn new() -> DataFrame {
        DataFrame {
            len: 0,
            columns: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn add_col<T>(&mut self, n: String, c: Col<T>) {
        if self.columns.is_empty() {
            self.len = c.len();
        } else {
            if c.len() != self.len() {
                panic!("column lengths don't match!");
            }
        }
        self.columns.insert(n, Arc::new(c));
    }

    pub fn get_col(&self, n: &str) -> ColRef {
        self.columns.get(n).unwrap().clone()
    }

}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_col_new() {
        Col::<u32>::new();
    }

    #[test]
    fn test_col_from_vec() {
        let c = Col(vec![0, 1, 2, 3]);
        assert_eq!(c.len(), 4);
    }

    #[test]
    fn test_col_iter() {
        let c = Col(vec![0.0, 1.0, 2.0, 3.0]);
        let cc = c.as_any().downcast_ref::<Col<f64>>().unwrap();
        assert_eq!(cc.len(), 4);
        assert_eq!(c.iter().sum::<f64>(), 6.0);
    
        //let sum: f64 = c.iter().sum();
        //assert_eq!(sum, 6.0);
    }

    #[test]
    fn test_dyn_col() {
        let c = Box::new(Col(vec![0.0, 1.0, 2.0, 3.0])) as Box<dyn Column>;
        let tot: f64 = c.as_vec::<f64>().iter().sum();
        assert_eq!(tot, 6.0);

        let tot: f64 = c.as_col::<f64>().iter().sum();
        assert_eq!(tot, 6.0);
    }

    #[test]
    fn test_df_columns() {
        let mut df = DataFrame::new();
        df.add_col("col1".to_string(), Col(vec![1,2,3,4]));
        df.add_col("col2".to_string(), Col(vec![1.0, 2.0, 3.0, 4.0]));
        assert_eq!(df.len(), 4);

        let csum: i32 = df.get_col("col1").as_col::<i32>().iter().sum();
        assert_eq!(csum, 10);
    }
}
