// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
//
// dyna/rw_lock.h
//   Writer-preferring reader/writer lock (pthread_rwlock with
//   PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP). With several receive threads the
//   replica handles hot messages under the shared side and view-change / recovery /
//   heartbeat work under the exclusive side; glibc's default (reader-preferring)
//   rwlock could starve the heartbeat tick under sustained load, which would look like
//   a dead peer to the others.

#ifndef _DYNA_RW_LOCK_H_
#define _DYNA_RW_LOCK_H_

#include <pthread.h>

namespace specpaxos {
namespace dyna {

class RwLock {
public:
    RwLock() {
        pthread_rwlockattr_t attr;
        pthread_rwlockattr_init(&attr);
#ifdef __GLIBC__
        pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif
        pthread_rwlock_init(&lock_, &attr);
        pthread_rwlockattr_destroy(&attr);
    }
    ~RwLock() { pthread_rwlock_destroy(&lock_); }
    RwLock(const RwLock &) = delete;
    RwLock &operator=(const RwLock &) = delete;

    void lock_shared() { pthread_rwlock_rdlock(&lock_); }
    void unlock_shared() { pthread_rwlock_unlock(&lock_); }
    void lock() { pthread_rwlock_wrlock(&lock_); }
    void unlock() { pthread_rwlock_unlock(&lock_); }

private:
    pthread_rwlock_t lock_;
};

} // namespace dyna
} // namespace specpaxos

#endif // _DYNA_RW_LOCK_H_
