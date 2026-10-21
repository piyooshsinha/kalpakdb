//! Conservative upload reservations shared by HTTP, gRPC and replication.
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct UploadBudget {
    available: Arc<Semaphore>,
    reservation: u32,
}

pub(crate) type UploadLease = Arc<OwnedSemaphorePermit>;

impl UploadBudget {
    pub(crate) fn new(max_block_bytes: usize) -> Result<Self, &'static str> {
        const MIB: usize = 1024 * 1024;
        // Reserve a full block plus one completed gRPC batch, rounding up.
        // Minimum 128 units also limits active uploads to four.
        let reservation = max_block_bytes
            .checked_add(64 * MIB)
            .and_then(|n| n.checked_add(MIB - 1))
            .ok_or("upload reservation overflow")?
            / MIB;
        if reservation > 512 {
            return Err("max block size exceeds shared upload budget (maximum 448 MiB)");
        }
        Ok(Self {
            available: Arc::new(Semaphore::new(512)),
            reservation: reservation.max(128) as u32,
        })
    }

    pub(crate) fn try_acquire(&self) -> Result<UploadLease, ()> {
        self.available
            .clone()
            .try_acquire_many_owned(self.reservation)
            .map(Arc::new)
            .map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_lease_holds_shared_capacity_until_last_owner_finishes() {
        let budget = UploadBudget::new(256 * 1024 * 1024).unwrap();
        let http = budget.try_acquire().unwrap();
        let replication = http.clone();
        assert!(budget.try_acquire().is_err());
        drop(http);
        assert!(budget.try_acquire().is_err());
        drop(replication);
        assert!(budget.try_acquire().is_ok());
    }

    #[test]
    fn small_uploads_have_bounded_concurrency_and_bad_configs_fail() {
        let budget = UploadBudget::new(1).unwrap();
        let leases: Vec<_> = (0..4).map(|_| budget.try_acquire().unwrap()).collect();
        assert!(budget.try_acquire().is_err());
        drop(leases);
        assert!(budget.try_acquire().is_ok());
        assert!(UploadBudget::new(usize::MAX).is_err());
        assert!(UploadBudget::new(449 * 1024 * 1024).is_err());
    }
}
