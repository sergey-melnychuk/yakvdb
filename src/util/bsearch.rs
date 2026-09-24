use std::cmp::Ordering;
use std::fmt::Debug;

/// Binary search over `lo..=hi`, where `cmp(i)` orders the key sought
/// against element `i`. Returns the index of an equal element, or where the
/// search ended: the first element above the key, or `hi` if none is.
pub(crate) fn bsearch<I: UInt>(mut lo: I, mut hi: I, cmp: impl Fn(I) -> Ordering) -> I {
    while lo < hi {
        let mid = lo + (hi - lo) / I::from(2);
        match cmp(mid) {
            Ordering::Less => {
                hi = mid;
            }
            Ordering::Greater => {
                lo = mid + I::from(1);
            }
            Ordering::Equal => return mid,
        }
    }
    assert_eq!(lo, hi);
    lo
}

// Trait to use as a bound for unsigned integer, inspired by:
// https://users.rust-lang.org/t/difficulty-creating-numeric-trait/34345/4
pub(crate) trait UInt:
    Copy
    + Ord
    + Sized
    + Debug
    + From<u8>
    + std::ops::Add<Output = Self>
    + std::ops::Sub<Output = Self>
    + std::ops::Div<Output = Self>
    + std::cmp::Eq
    + std::cmp::PartialEq<Self>
{
}

impl UInt for u8 {}
impl UInt for u16 {}
impl UInt for u32 {}
impl UInt for u64 {}
impl UInt for u128 {}
