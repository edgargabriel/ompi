/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2009 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2006      Voltaire. All rights reserved.
 * Copyright (c) 2007      Mellanox Technologies. All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2012-2015 NVIDIA Corporation.  All rights reserved.
 * Copyright (c) 2015      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2022      Amazon.com, Inc. or its affiliates.  All Rights reserved.
 * Copyright (c) 2023      Advanced Micro Devices, Inc. All Rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file:
 *
 * This file implements a simple memory pool that is used by the GPU
 * buffer on the sending side.  It just gets a memory handle and event
 * handle that can be sent to the remote side which can then use the
 * handles to get access to the memory and the event to determine when
 * it can start accessing the memory.  There is no caching of the
 * memory handles as getting new ones is fast.  The event handles are
 * cached by the cuda_common code.
 */

#include "opal_config.h"
#include "opal/mca/rcache/base/base.h"
#include "opal/mca/rcache/gpurocsm/rcache_gpurocsm.h"
#include "opal/opal_rocm.h"

/* Not interested in warnings generated in hip_runtime_api.h */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundef"
#pragma GCC diagnostic ignored "-Wpedantic"
#pragma GCC diagnostic ignored "-Wstrict-prototypes"
#include <hip/hip_runtime.h>
/* Restore warnings to original state */
#pragma GCC diagnostic pop

/**
 * Called when the registration free list is created.  An event is created
 * for each entry.
 */
static void mca_rcache_gpurocsm_registration_constructor(mca_rcache_gpurocsm_registration_t *item)
{
    uintptr_t *event = &item->event;
    void *handle = (void *) &item->evtHandle;
    hipError_t result;

    result = hipEventCreateWithFlags((hipEvent_t *) event,
                                  hipEventInterprocess | hipEventDisableTiming);
    if (OPAL_UNLIKELY(hipSuccess != result)) {
        opal_output(0, "hipEventCreateWithFlags failed\n");
    }

    result = hipIpcGetEventHandle((hipIpcEventHandle_t *) handle, (hipEvent_t) *event);
    if (OPAL_UNLIKELY(hipSuccess != result)) {
        opal_output(0, "hipIpcGetEventHandle failed\n");
    }
}

/**
 * Called when the program is exiting.  This destroys the events.
 */
static void mca_rcache_gpurocsm_registration_destructor(mca_rcache_gpurocsm_registration_t *item)
{
    uintptr_t event = item->event;
    hipError_t result;

    result = hipEventDestroy((hipEvent_t) event);
    if (OPAL_UNLIKELY(hipSuccess != result)) {
        opal_output(0, "hipEventDestroy failed");
    }
}

OBJ_CLASS_INSTANCE(mca_rcache_gpurocsm_registration_t, mca_rcache_base_registration_t,
                   mca_rcache_gpurocsm_registration_constructor,
                   mca_rcache_gpurocsm_registration_destructor);

/*
 *  Initializes the rcache module.
 */
void mca_rcache_gpurocsm_module_init(mca_rcache_gpurocsm_module_t *rcache)
{
    rcache->super.rcache_component = &mca_rcache_gpurocsm_component.super;
    rcache->super.rcache_register = mca_rcache_gpurocsm_register;
    rcache->super.rcache_find = mca_rcache_gpurocsm_find;
    rcache->super.rcache_deregister = mca_rcache_gpurocsm_deregister;
    rcache->super.rcache_finalize = mca_rcache_gpurocsm_finalize;

    OBJ_CONSTRUCT(&rcache->reg_list, opal_free_list_t);

    /* Start with 0 entries in the free list since CUDA may not have
     * been initialized when this free list is created and there is
     * some CUDA specific activities that need to be done. */
    opal_free_list_init(&rcache->reg_list, sizeof(struct mca_opal_rocm_reg_t),
                        opal_cache_line_size, OBJ_CLASS(mca_rcache_gpurocsm_registration_t), 0,
                        opal_cache_line_size, 0, -1, 64, NULL, 0, NULL, NULL, NULL);
}

/**
 * Just go ahead and get a new registration.  The find and register
 * functions are the same thing for this memory pool.
 */
int mca_rcache_gpurocsm_find(mca_rcache_base_module_t *rcache, void *addr, size_t size,
                          mca_rcache_base_registration_t **reg)
{
    return mca_rcache_gpurocsm_register(rcache, addr, size, 0, 0, reg);
}

/*
 * Get the memory handle of a local section of memory that can be sent
 * to the remote size so it can access the memory.  This is the
 * registration function for the sending side of a message transfer.
 */
static int mca_rcache_gpurocsm_get_mem_handle(void *base, size_t size, mca_rcache_base_registration_t *newreg)
{
    hipMemoryType memType;
    hipError_t result;
    hipIpcMemHandle_t *memHandle;
    hipDeviceptr_t pbase;
    size_t psize;

    mca_opal_rocm_reg_t *cuda_reg = (mca_opal_rocm_reg_t *) newreg;
    memHandle = (hipIpcMemHandle_t *) cuda_reg->data.memHandle;

    /* We should only be there if this is a CUDA device pointer */
    result = hipPointerGetAttribute(&memType, HIP_POINTER_ATTRIBUTE_MEMORY_TYPE,
                                          (hipDeviceptr_t) base);
    assert(hipSuccess == result);
    assert(hipMemoryTypeDevice == memType);

    /* Get the memory handle so we can send it to the remote process. */
    result = hipIpcGetMemHandle(memHandle, (hipDeviceptr_t) base);

    if (hipSuccess != result) {
        return OPAL_ERROR;
    }

    /* Need to get the real base and size of the memory handle.  This is
     * how the remote side saves the handles in a cache. */
    result = hipMemGetAddressRange(&pbase, &psize, (hipDeviceptr_t) base);
    if (hipSuccess != result) {
        return OPAL_ERROR;
    }

    /* Store all the information in the registration */
    cuda_reg->base.base = (void *) pbase;
    cuda_reg->base.bound = (unsigned char *) pbase + psize - 1;
    cuda_reg->data.memh_seg_addr.pval = (void *) pbase;
    cuda_reg->data.memh_seg_len = psize;

    //#if OPAL_CUDA_SYNC_MEMOPS
    /* With CUDA 6.0, we can set an attribute on the memory pointer that will
     * ensure any synchronous copies are completed prior to any other access
     * of the memory region.  This means we do not need to record an event
     * and send to the remote side.
     */
    //memType = 1; /* Just use this variable since we already have it */
    //result = cuPointerSetAttribute(&memType, HIP_POINTER_ATTRIBUTE_SYNC_MEMOPS,
    //                                      (hipDeviceptr_t) base);
    //if (OPAL_UNLIKELY(hipSuccess != result)) {
    //    return OPAL_ERROR;
    //}
    //#else
    
    /* Need to record the event to ensure that any memcopies into the
     * device memory have completed.  The event handle associated with
     * this event is sent to the remote process so that it will wait
     * on this event prior to copying data out of the device memory.
     * Note that this needs to be the NULL stream to make since it is
     * unknown what stream any copies into the device memory were done
     * with. */
    result = hipEventRecord((hipEvent_t) cuda_reg->data.event, 0);
    if (OPAL_UNLIKELY(hipSuccess != result)) {
        return OPAL_ERROR;
    }
    //#endif /* OPAL_CUDA_SYNC_MEMOPS */

    return OPAL_SUCCESS;
}

/*
 * This is the one function that does all the work.  It will call into
 * the register function to get the memory handle for the sending
 * buffer.  There is no need to deregister the memory handle so the
 * deregister function is a no-op.
 */
int mca_rcache_gpurocsm_register(mca_rcache_base_module_t *rcache, void *addr, size_t size,
                              uint32_t flags, int32_t access_flags,
                              mca_rcache_base_registration_t **reg)
{
    mca_rcache_gpurocsm_module_t *rcache_gpurocsm = (mca_rcache_gpurocsm_module_t *) rcache;
    mca_rcache_base_registration_t *gpurocsm_reg;
    opal_free_list_item_t *item;
    unsigned char *base, *bound;
    int rc;

    /* In spite of the fact we return an error code, the existing code
     * checks the registration for a NULL value rather than looking at
     * the return code.  So, initialize the registration to NULL in
     * case we run into a failure. */
    *reg = NULL;

    base = addr;
    bound = (unsigned char *) addr + size - 1;

    item = opal_free_list_get(&rcache_gpurocsm->reg_list);
    if (NULL == item) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }
    gpurocsm_reg = (mca_rcache_base_registration_t *) item;

    gpurocsm_reg->rcache = rcache;
    gpurocsm_reg->base = base;
    gpurocsm_reg->bound = bound;
    gpurocsm_reg->flags = flags;
    gpurocsm_reg->access_flags = access_flags;

    rc = mca_rcache_gpurocsm_get_mem_handle(base, size, gpurocsm_reg);

    if (rc != OPAL_SUCCESS) {
        opal_free_list_return(&rcache_gpurocsm->reg_list, item);
        return rc;
    }

    *reg = gpurocsm_reg;
    (*reg)->ref_count++;
    return OPAL_SUCCESS;
}

/*
 * Return the registration to the free list.
 */
int mca_rcache_gpurocsm_deregister(struct mca_rcache_base_module_t *rcache,
                                mca_rcache_base_registration_t *reg)
{
    mca_rcache_gpurocsm_module_t *rcache_gpurocsm = (mca_rcache_gpurocsm_module_t *) rcache;

    opal_free_list_return(&rcache_gpurocsm->reg_list, (opal_free_list_item_t *) reg);
    return OPAL_SUCCESS;
}

/**
 * Free up the resources.
 */
void mca_rcache_gpurocsm_finalize(struct mca_rcache_base_module_t *rcache)
{
    opal_free_list_item_t *item;
    mca_rcache_gpurocsm_module_t *rcache_gpurocsm = (mca_rcache_gpurocsm_module_t *) rcache;

    /* Need to run the destructor on each item in the free list explicitly.
     * The destruction of the free list only runs the destructor on the
     * main free list, not each item. */
    while (NULL
           != (item = (opal_free_list_item_t *) opal_lifo_pop(&(rcache_gpurocsm->reg_list.super)))) {
        OBJ_DESTRUCT(item);
    }

    OBJ_DESTRUCT(&rcache_gpurocsm->reg_list);
    return;
}
