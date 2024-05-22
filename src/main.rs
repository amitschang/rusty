use rusttest::compute::process_array_t;
use rusttest::enum_df::{Column, DataFrame};

pub fn main() {
    let mut df = DataFrame::new();
    df.addcol("col1", Column::F(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0])).unwrap();

    // mutable borrow of array so cannot have immutable borrow live past here
    if let Column::F(c) = df.get_mut("col1").unwrap() {
        process_array_t(c);
    }

    // now we can do the mutable borrow, and this can be used
    let Column::F(c) = df.get("col1").unwrap() else {
        panic!("failed to get expected F array");
    };
    // we can still use it since it is in the outer scope of let
    println!("after process_array: {:?}", c);
}
