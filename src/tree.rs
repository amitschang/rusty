use std::{
    sync::{mpsc, Arc, Mutex},
    thread,
};

/// given an array of index positions that represent a sort order, reorder given
/// array by those indexes. Buffer is a slice of the same len as input that represents
/// a temp workspace (passed in so we can share that amongst df columns)
fn sort_by_indexes(idxs: &[usize], a: &mut [i32], buffer: &mut [i32]) {
    for (i, idx) in idxs.iter().enumerate() {
        buffer[i] = a[i];
        if *idx > i {
            a[i] = a[*idx];
        } else if *idx < i {
            a[i] = buffer[*idx];
        }
    }
}

/// sort a dataframe by specific column index in place
fn co_sort(df: &mut Vec<&mut [i32]>, by: usize) {
    let mut arr_sort = (0..df[0].len()).collect::<Vec<_>>();
    arr_sort.sort_by_key(|k| df[by][*k]);
    let mut sort_buffer = Vec::new();
    sort_buffer.resize(df[0].len(), 0);
    for i in 0..df.len() {
        sort_by_indexes(&arr_sort, &mut df[i], &mut sort_buffer);
    }
}

/// Sort dataframe by some column (index in vec of vecs), then split at some point.
/// This emulates a tree split, and the co sorting enables carrying different dimensions
/// down the tree.
fn multi_sort_and_split(mut df: Vec<&mut [i32]>, by: usize) -> (Vec<&mut [i32]>, Vec<&mut [i32]>){
    let mut out_a = Vec::new();
    let mut out_b = Vec::new();
    let sp = df[0].len() / 2;
    co_sort(&mut df, by);
    for c in df {
        let (a, b) = c.split_at_mut(sp);
        out_a.push(a);
        out_b.push(b);
    }
    (out_a, out_b)
}

/// work scheduling for iterative splits of input dataframe. Sends vectors of
/// mutable slices (representing split nodes of dataframe) through channels to
/// thread pool. Right now does this a fixed number of times for demo, presumably
/// could:
///
/// * loop forever in thread but have stopping condition (e.g. leaf size)
/// * wrap df in struct that tracks dimension, in order to split on another one
///   in subsequent iterations
/// * write tree structure after finding split
pub fn par_mut_sorter_multi_arr(arr: &mut Vec<Vec<i32>>) {
    let (send, recv) = mpsc::channel();
    let recv = Arc::new(Mutex::new(recv));
    let send = Arc::new(send);
    thread::scope(|scope| {
        for _i in 0..=1 {
            let rc = Arc::clone(&recv);
            let sc = Arc::clone(&send);
            scope.spawn(move || {
                let r = rc.lock().unwrap().recv().unwrap();
                let (a, b) = multi_sort_and_split(r, _i as usize);
                sc.send(a).unwrap();
                sc.send(b).unwrap();
            });
        }
        let mut init = Vec::new();
        for i in arr.iter_mut() {
            init.push(&mut i[..]);
        }
        send.send(init).unwrap();
    });
    let recv_u = recv.lock().unwrap();
    loop {
        match recv_u.try_recv() {
            Ok(a) => println!("got arr chunk: {:?}", a),
            Err(_) => break,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_arr_sort() {
        let mut in_array = vec![1,2,3,4,4,3,2,1];
        let sort_indexes:Vec<usize> = vec![0,7,1,6,2,5,3,4];

        let mut buffer: Vec<i32> = in_array.iter().map(|_| 0).collect();
        sort_by_indexes(&sort_indexes, &mut in_array, &mut buffer);
        assert_eq!(in_array, vec![1,1,2,2,3,3,4,4]);
    }

    #[test]
    fn test_co_sort() {
        let mut df = vec![
            vec![1,2,3,4,4,3,2,1],
            vec![5,4,3,2,2,3,4,5],
            vec![9,3,2,8,5,7,1,0]
        ];
        let mut df_ref = Vec::from_iter(df.iter_mut().map(|x| x.as_mut_slice()));
        co_sort(&mut df_ref, 0);
        assert_eq!(df[1], vec![5,5,4,4,3,3,2,2]);
    }

    #[test]
    fn test_multi() {
        let mut df = vec![
            vec![1,2,3,4,4,3,2,1],
            vec![5,4,3,2,2,3,4,5],
            vec![9,3,2,8,5,7,1,0]
        ];
        par_mut_sorter_multi_arr(&mut df);
    }
}
