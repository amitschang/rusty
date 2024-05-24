use std::{
    sync::{atomic::AtomicI32, mpsc, Arc, Mutex},
    thread, usize,
};

const NTHREADS: u32 = 4;

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

/// Work management tracking structure. This is passed between threads to request
/// a split be worked on and indicates the dimension on which the split was originally
/// created (or usize MAX if this is the first)
#[derive(Debug)]
struct WorkUnit<'a> {
    df: Vec<&'a mut [i32]>,
    split_dim: usize,
}

impl WorkUnit<'_> {
    // A dummy round robin dimension selection
    fn new_split_dim(&self) -> usize {
        let dim = match self.split_dim {
            usize::MAX => 0,
            _ => self.split_dim + 1,
        };
        match dim >= self.df.len() {
            true => 0,
            false => dim,
        }
    }
}

/// work scheduling for iterative splits of input dataframe. Sends vectors of
/// mutable slices (representing split nodes of dataframe) through channels to
/// thread pool.
///
/// TODO:
///
/// * write tree structure after finding split
pub fn par_mut_sorter_multi_arr(arr: &mut Vec<Vec<i32>>) {
    let (send, recv) = mpsc::channel();
    let recv = Arc::new(Mutex::new(recv));
    let send = Arc::new(send);
    let work_counter = Arc::new(AtomicI32::new(1));  // needs to be able to go negative
    thread::scope(|scope| {
        for _i in 0..NTHREADS {
            let rc = Arc::clone(&recv);
            let sc = Arc::clone(&send);
            let wc = Arc::clone(&work_counter);
            scope.spawn(move || {
                loop {
                    let r: WorkUnit = { rc.lock().unwrap().recv().unwrap() };
                    // stoping condition, here we just fake it with a condition
                    // of a min leaf size of 2.
                    if r.df[0].len() < 3 {
                        // only terminate if there is no pending work left, i.e.
                        // the value before sub is 1 or less
                        let pending = wc.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                        if pending <= 1 {
                            // signal is vector of single empty slice, passing above
                            // condition - probably a better mechanism exists.
                            let wu = WorkUnit { df: vec!(&mut [] as &mut[i32]), split_dim: 0 };
                            sc.send(wu).unwrap();
                            break;
                        }
                        continue;
                    }
                    wc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let split_dim = r.new_split_dim();
                    let (a, b) = multi_sort_and_split(r.df, split_dim);
                    sc.send(WorkUnit { df: a, split_dim}).unwrap();
                    sc.send(WorkUnit { df: b, split_dim}).unwrap();
            }
            });
        }
        let init = WorkUnit {
            df: arr.iter_mut().map(|x| x.as_mut_slice()).collect::<Vec<_>>(),
            split_dim: usize::MAX,
        };
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
            vec![1,2,3,4,4,3,2,1,99],
            vec![5,4,3,2,2,3,4,5,-1],
            vec![9,3,2,8,5,7,1,0,99]
        ];
        par_mut_sorter_multi_arr(&mut df);
        assert_eq!(df, vec![
            vec![2, 2, 1, 1, 99, 4, 3, 4, 3],
            vec![4, 4, 5, 5, -1, 2, 3, 2, 3],
            vec![3, 1, 9, 0, 99, 8, 2, 5, 7]]);
        println!("at end: {:?}", df);
    }
}
