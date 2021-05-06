/**
 *    Copyright (C) 2020-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kTest

#include "mongo/platform/basic.h"

#include <boost/optional/optional_io.hpp>

#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/op_observer_noop.h"
#include "mongo/db/op_observer_registry.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_request.h"
#include "mongo/db/repl/drop_pending_collection_reaper.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/primary_only_service_test_fixture.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/repl/storage_interface_mock.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/db/s/resharding/resharding_data_copy_util.h"
#include "mongo/db/s/resharding/resharding_donor_service.h"
#include "mongo/db/s/resharding/resharding_service_test_helpers.h"
#include "mongo/db/s/resharding_util.h"
#include "mongo/logv2/log.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using DonorStateTransitionController =
    resharding_service_test_helpers::StateTransitionController<DonorStateEnum>;
using OpObserverForTest =
    resharding_service_test_helpers::OpObserverForTest<DonorStateEnum, ReshardingDonorDocument>;
using PauseDuringStateTransitions =
    resharding_service_test_helpers::PauseDuringStateTransitions<DonorStateEnum>;

const ShardId donorShardId{"myShardId"};

class ExternalStateForTest : public ReshardingDonorService::DonorStateMachineExternalState {
public:
    ShardId myShardId(ServiceContext* serviceContext) const override {
        return donorShardId;
    }

    void refreshCatalogCache(OperationContext* opCtx, const NamespaceString& nss) override {}

    void waitForCollectionFlush(OperationContext* opCtx, const NamespaceString& nss) override {}

    void updateCoordinatorDocument(OperationContext* opCtx,
                                   const BSONObj& query,
                                   const BSONObj& update) override {}
};

class DonorOpObserverForTest : public OpObserverForTest {
public:
    DonorOpObserverForTest(std::shared_ptr<DonorStateTransitionController> controller)
        : OpObserverForTest(std::move(controller),
                            NamespaceString::kDonorReshardingOperationsNamespace) {}

    DonorStateEnum getState(const ReshardingDonorDocument& donorDoc) override {
        return donorDoc.getMutableState().getState();
    }
};

class ReshardingDonorServiceForTest : public ReshardingDonorService {
public:
    explicit ReshardingDonorServiceForTest(ServiceContext* serviceContext)
        : ReshardingDonorService(serviceContext) {}

    std::shared_ptr<PrimaryOnlyService::Instance> constructInstance(BSONObj initialState) override {
        return std::make_shared<DonorStateMachine>(
            this,
            ReshardingDonorDocument::parse({"ReshardingDonorServiceForTest"}, initialState),
            std::make_unique<ExternalStateForTest>());
    }
};

class ReshardingDonorServiceTest : public repl::PrimaryOnlyServiceMongoDTest {
public:
    using DonorStateMachine = ReshardingDonorService::DonorStateMachine;

    std::unique_ptr<repl::PrimaryOnlyService> makeService(ServiceContext* serviceContext) override {
        return std::make_unique<ReshardingDonorServiceForTest>(serviceContext);
    }

    void setUp() override {
        repl::PrimaryOnlyServiceMongoDTest::setUp();

        auto serviceContext = getServiceContext();
        auto storageMock = std::make_unique<repl::StorageInterfaceMock>();
        repl::DropPendingCollectionReaper::set(
            serviceContext, std::make_unique<repl::DropPendingCollectionReaper>(storageMock.get()));
        repl::StorageInterface::set(serviceContext, std::move(storageMock));

        _controller = std::make_shared<DonorStateTransitionController>();
        _opObserverRegistry->addObserver(std::make_unique<DonorOpObserverForTest>(_controller));
    }

    DonorStateTransitionController* controller() {
        return _controller.get();
    }

    ReshardingDonorDocument makeStateDocument(bool isAlsoRecipient) {
        DonorShardContext donorCtx;
        donorCtx.setState(DonorStateEnum::kPreparingToDonate);

        ReshardingDonorDocument doc(std::move(donorCtx),
                                    {ShardId{"recipient1"},
                                     isAlsoRecipient ? donorShardId : ShardId{"recipient2"},
                                     ShardId{"recipient3"}});

        NamespaceString sourceNss("sourcedb.sourcecollection");
        auto sourceUUID = UUID::gen();
        auto commonMetadata =
            CommonReshardingMetadata(UUID::gen(),
                                     sourceNss,
                                     sourceUUID,
                                     constructTemporaryReshardingNss(sourceNss.db(), sourceUUID),
                                     BSON("newKey" << 1));

        doc.setCommonReshardingMetadata(std::move(commonMetadata));
        return doc;
    }

    void createSourceCollection(OperationContext* opCtx, const ReshardingDonorDocument& donorDoc) {
        CollectionOptions options;
        options.uuid = donorDoc.getSourceUUID();
        resharding::data_copy::ensureCollectionDropped(opCtx, donorDoc.getSourceNss());
        resharding::data_copy::ensureCollectionExists(opCtx, donorDoc.getSourceNss(), options);
    }

    void createTemporaryReshardingCollection(OperationContext* opCtx,
                                             const ReshardingDonorDocument& donorDoc) {
        CollectionOptions options;
        options.uuid = donorDoc.getReshardingUUID();
        resharding::data_copy::ensureCollectionDropped(opCtx, donorDoc.getTempReshardingNss());
        resharding::data_copy::ensureCollectionExists(
            opCtx, donorDoc.getTempReshardingNss(), options);
    }

    void notifyRecipientsDoneCloning(OperationContext* opCtx,
                                     DonorStateMachine& donor,
                                     const ReshardingDonorDocument& donorDoc) {
        _onReshardingFieldsChanges(opCtx, donor, donorDoc, CoordinatorStateEnum::kApplying);
    }

    void notifyToStartBlockingWrites(OperationContext* opCtx,
                                     DonorStateMachine& donor,
                                     const ReshardingDonorDocument& donorDoc) {
        _onReshardingFieldsChanges(opCtx, donor, donorDoc, CoordinatorStateEnum::kBlockingWrites);
    }

    void notifyReshardingCommitting(OperationContext* opCtx,
                                    DonorStateMachine& donor,
                                    const ReshardingDonorDocument& donorDoc) {
        _onReshardingFieldsChanges(opCtx, donor, donorDoc, CoordinatorStateEnum::kCommitting);
    }

    void notifyReshardingAborting(OperationContext* opCtx,
                                  DonorStateMachine& donor,
                                  const ReshardingDonorDocument& donorDoc) {
        _onReshardingFieldsChanges(opCtx, donor, donorDoc, CoordinatorStateEnum::kAborting);
    }

    void checkStateDocumentRemoved(OperationContext* opCtx) {
        AutoGetCollection donorColl(
            opCtx, NamespaceString::kDonorReshardingOperationsNamespace, MODE_IS);
        ASSERT_TRUE(bool(donorColl));
        ASSERT_TRUE(bool(donorColl->isEmpty(opCtx)));
    }

private:
    void _onReshardingFieldsChanges(OperationContext* opCtx,
                                    DonorStateMachine& donor,
                                    const ReshardingDonorDocument& donorDoc,
                                    CoordinatorStateEnum coordinatorState) {
        auto reshardingFields = TypeCollectionReshardingFields{donorDoc.getReshardingUUID()};
        auto donorFields = TypeCollectionDonorFields{donorDoc.getTempReshardingNss(),
                                                     donorDoc.getReshardingKey(),
                                                     donorDoc.getRecipientShards()};
        reshardingFields.setDonorFields(donorFields);
        reshardingFields.setState(coordinatorState);
        donor.onReshardingFieldsChanges(opCtx, reshardingFields);
    }

    std::shared_ptr<DonorStateTransitionController> _controller;
};

TEST_F(ReshardingDonorServiceTest, CanTransitionThroughEachStateToCompletion) {
    for (bool isAlsoRecipient : {false, true}) {
        LOGV2(5641800,
              "Running case",
              "test"_attr = _agent.getTestName(),
              "isAlsoRecipient"_attr = isAlsoRecipient);

        auto doc = makeStateDocument(isAlsoRecipient);
        auto opCtx = makeOperationContext();

        createSourceCollection(opCtx.get(), doc);
        if (isAlsoRecipient) {
            createTemporaryReshardingCollection(opCtx.get(), doc);
        }

        DonorStateMachine::insertStateDocument(opCtx.get(), doc);
        auto donor = DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());

        notifyRecipientsDoneCloning(opCtx.get(), *donor, doc);
        notifyToStartBlockingWrites(opCtx.get(), *donor, doc);
        notifyReshardingCommitting(opCtx.get(), *donor, doc);

        ASSERT_OK(donor->getCompletionFuture().getNoThrow());
        checkStateDocumentRemoved(opCtx.get());
    }
}

TEST_F(ReshardingDonorServiceTest, WritesNoOpOplogEntryToGenerateMinFetchTimestamp) {
    boost::optional<PauseDuringStateTransitions> donatingInitialDataTransitionGuard;
    donatingInitialDataTransitionGuard.emplace(controller(), DonorStateEnum::kDonatingInitialData);

    auto doc = makeStateDocument(false /* isAlsoRecipient */);
    auto opCtx = makeOperationContext();
    DonorStateMachine::insertStateDocument(opCtx.get(), doc);
    auto donor = DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());

    donatingInitialDataTransitionGuard->wait(DonorStateEnum::kDonatingInitialData);
    stepDown();
    donatingInitialDataTransitionGuard.reset();

    ASSERT_EQ(donor->getCompletionFuture().getNoThrow(),
              ErrorCodes::InterruptedDueToReplStateChange);

    DBDirectClient client(opCtx.get());
    auto cursor =
        client.query(NamespaceString(NamespaceString::kRsOplogNamespace.ns()),
                     BSON("ns" << NamespaceString::kForceOplogBatchBoundaryNamespace.ns()));

    ASSERT_TRUE(cursor->more()) << "Found no oplog entries for source collection";
    repl::OplogEntry op(cursor->next());
    ASSERT_FALSE(cursor->more()) << "Found multiple oplog entries for source collection: "
                                 << op.getEntry() << " and " << cursor->nextSafe();

    ASSERT_EQ(OpType_serializer(op.getOpType()), OpType_serializer(repl::OpTypeEnum::kNoop))
        << op.getEntry();
    ASSERT_FALSE(op.getUuid()) << op.getEntry();
    ASSERT_EQ(op.getObject()["msg"].type(), BSONType::String) << op.getEntry();
    ASSERT_FALSE(bool(op.getObject2())) << op.getEntry();
    ASSERT_FALSE(bool(op.getDestinedRecipient())) << op.getEntry();
}

TEST_F(ReshardingDonorServiceTest, WritesFinalReshardOpOplogEntriesWhileWritesBlocked) {
    boost::optional<PauseDuringStateTransitions> blockingWritesTransitionGuard;
    blockingWritesTransitionGuard.emplace(controller(), DonorStateEnum::kBlockingWrites);

    auto doc = makeStateDocument(false /* isAlsoRecipient */);
    auto opCtx = makeOperationContext();
    DonorStateMachine::insertStateDocument(opCtx.get(), doc);
    auto donor = DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());

    notifyRecipientsDoneCloning(opCtx.get(), *donor, doc);
    notifyToStartBlockingWrites(opCtx.get(), *donor, doc);

    blockingWritesTransitionGuard->wait(DonorStateEnum::kBlockingWrites);
    stepDown();
    blockingWritesTransitionGuard.reset();

    ASSERT_EQ(donor->getCompletionFuture().getNoThrow(),
              ErrorCodes::InterruptedDueToReplStateChange);

    DBDirectClient client(opCtx.get());
    auto cursor = client.query(NamespaceString(NamespaceString::kRsOplogNamespace.ns()),
                               BSON("ns" << doc.getSourceNss().toString()));

    ASSERT_TRUE(cursor->more()) << "Found no oplog entries for source collection";

    for (const auto& recipientShardId : doc.getRecipientShards()) {
        ASSERT_TRUE(cursor->more()) << "Didn't find finalReshardOp entry for source collection";
        repl::OplogEntry op(cursor->next());

        ASSERT_EQ(OpType_serializer(op.getOpType()), OpType_serializer(repl::OpTypeEnum::kNoop))
            << op.getEntry();
        ASSERT_EQ(op.getUuid(), doc.getSourceUUID()) << op.getEntry();
        ASSERT_EQ(op.getDestinedRecipient(), recipientShardId) << op.getEntry();
        ASSERT_EQ(op.getObject()["msg"].type(), BSONType::String) << op.getEntry();
        ASSERT_TRUE(bool(op.getObject2())) << op.getEntry();
        ASSERT_BSONOBJ_BINARY_EQ(*op.getObject2(),
                                 BSON("type"
                                      << "reshardFinalOp"
                                      << "reshardingUUID" << doc.getReshardingUUID()));
    }

    ASSERT_FALSE(cursor->more()) << "Found extra oplog entry for source collection: "
                                 << cursor->nextSafe();
}

TEST_F(ReshardingDonorServiceTest, StepDownStepUpEachTransition) {
    const std::vector<DonorStateEnum> donorStates{DonorStateEnum::kDonatingInitialData,
                                                  DonorStateEnum::kDonatingOplogEntries,
                                                  DonorStateEnum::kBlockingWrites,
                                                  DonorStateEnum::kDone};

    for (bool isAlsoRecipient : {false, true}) {
        LOGV2(5641801,
              "Running case",
              "test"_attr = _agent.getTestName(),
              "isAlsoRecipient"_attr = isAlsoRecipient);

        PauseDuringStateTransitions stateTransitionsGuard{controller(), donorStates};
        auto doc = makeStateDocument(isAlsoRecipient);
        auto instanceId =
            BSON(ReshardingDonorDocument::kReshardingUUIDFieldName << doc.getReshardingUUID());

        auto opCtx = makeOperationContext();

        auto prevState = DonorStateEnum::kUnused;
        for (const auto state : donorStates) {
            auto donor = [&] {
                if (prevState == DonorStateEnum::kUnused) {
                    createSourceCollection(opCtx.get(), doc);
                    if (isAlsoRecipient) {
                        createTemporaryReshardingCollection(opCtx.get(), doc);
                    }

                    DonorStateMachine::insertStateDocument(opCtx.get(), doc);
                    return DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());
                } else {
                    auto maybeDonor = DonorStateMachine::lookup(opCtx.get(), _service, instanceId);
                    ASSERT_TRUE(bool(maybeDonor));

                    // Allow the transition to prevState to succeed on this primary-only service
                    // instance.
                    stateTransitionsGuard.unset(prevState);
                    return *maybeDonor;
                }
            }();

            // Signal a change in the coordinator's state for the donor state transition dependent
            // on it.
            switch (state) {
                case DonorStateEnum::kDonatingOplogEntries: {
                    notifyRecipientsDoneCloning(opCtx.get(), *donor, doc);
                    break;
                }
                case DonorStateEnum::kBlockingWrites: {
                    notifyToStartBlockingWrites(opCtx.get(), *donor, doc);
                    break;
                }
                case DonorStateEnum::kDone: {
                    notifyReshardingCommitting(opCtx.get(), *donor, doc);
                    break;
                }
                default:
                    break;
            }

            // Step down before the transition to state can complete.
            stateTransitionsGuard.wait(state);
            stepDown();

            ASSERT_EQ(donor->getCompletionFuture().getNoThrow(),
                      ErrorCodes::InterruptedDueToReplStateChange);

            prevState = state;

            donor.reset();
            stepUp(opCtx.get());
        }

        // Finally complete the operation and ensure its success.
        auto maybeDonor = DonorStateMachine::lookup(opCtx.get(), _service, instanceId);
        ASSERT_TRUE(bool(maybeDonor));

        auto donor = *maybeDonor;
        stateTransitionsGuard.unset(DonorStateEnum::kDone);

        notifyReshardingCommitting(opCtx.get(), *donor, doc);
        ASSERT_OK(donor->getCompletionFuture().getNoThrow());
        checkStateDocumentRemoved(opCtx.get());
    }
}

TEST_F(ReshardingDonorServiceTest, DropsSourceCollectionWhenDone) {
    auto doc = makeStateDocument(false /* isAlsoRecipient */);
    auto opCtx = makeOperationContext();

    createSourceCollection(opCtx.get(), doc);

    DonorStateMachine::insertStateDocument(opCtx.get(), doc);
    auto donor = DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());

    notifyRecipientsDoneCloning(opCtx.get(), *donor, doc);
    notifyToStartBlockingWrites(opCtx.get(), *donor, doc);

    {
        AutoGetCollection coll(opCtx.get(), doc.getSourceNss(), MODE_IS);
        ASSERT_TRUE(bool(coll));
        ASSERT_EQ(coll->uuid(), doc.getSourceUUID());
    }

    notifyReshardingCommitting(opCtx.get(), *donor, doc);
    ASSERT_OK(donor->getCompletionFuture().getNoThrow());
    checkStateDocumentRemoved(opCtx.get());

    {
        AutoGetCollection coll(opCtx.get(), doc.getSourceNss(), MODE_IS);
        ASSERT_FALSE(bool(coll));
    }
}

TEST_F(ReshardingDonorServiceTest, RenamesTemporaryReshardingCollectionWhenDone) {
    auto doc = makeStateDocument(true /* isAlsoRecipient */);
    auto opCtx = makeOperationContext();

    createSourceCollection(opCtx.get(), doc);
    createTemporaryReshardingCollection(opCtx.get(), doc);

    DonorStateMachine::insertStateDocument(opCtx.get(), doc);
    auto donor = DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());

    notifyRecipientsDoneCloning(opCtx.get(), *donor, doc);
    notifyToStartBlockingWrites(opCtx.get(), *donor, doc);

    {
        AutoGetCollection coll(opCtx.get(), doc.getSourceNss(), MODE_IS);
        ASSERT_TRUE(bool(coll));
        ASSERT_EQ(coll->uuid(), doc.getSourceUUID());
    }

    notifyReshardingCommitting(opCtx.get(), *donor, doc);
    ASSERT_OK(donor->getCompletionFuture().getNoThrow());
    checkStateDocumentRemoved(opCtx.get());

    {
        AutoGetCollection coll(opCtx.get(), doc.getSourceNss(), MODE_IS);
        ASSERT_TRUE(bool(coll));
        ASSERT_EQ(coll->uuid(), doc.getReshardingUUID());
    }
}

TEST_F(ReshardingDonorServiceTest, CompletesWithStepdownAfterAbort) {
    for (bool isAlsoRecipient : {false, true}) {
        LOGV2(5641802,
              "Running case",
              "test"_attr = _agent.getTestName(),
              "isAlsoRecipient"_attr = isAlsoRecipient);

        boost::optional<PauseDuringStateTransitions> doneTransitionGuard;
        doneTransitionGuard.emplace(controller(), DonorStateEnum::kDone);

        auto doc = makeStateDocument(isAlsoRecipient);
        auto instanceId =
            BSON(ReshardingDonorDocument::kReshardingUUIDFieldName << doc.getReshardingUUID());

        auto opCtx = makeOperationContext();

        createSourceCollection(opCtx.get(), doc);
        if (isAlsoRecipient) {
            createTemporaryReshardingCollection(opCtx.get(), doc);
        }

        DonorStateMachine::insertStateDocument(opCtx.get(), doc);
        auto donor = DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());

        notifyRecipientsDoneCloning(opCtx.get(), *donor, doc);
        // The call to notifyToStartBlockingWrites() is skipped here because the donor is being
        // notified that the resharding operation is aborting before the donor would have
        // transitioned to kBlockingWrites.
        notifyReshardingAborting(opCtx.get(), *donor, doc);

        // Step down before the transition to kDone can complete.
        doneTransitionGuard->wait(DonorStateEnum::kDone);
        stepDown();

        ASSERT_EQ(donor->getCompletionFuture().getNoThrow(),
                  ErrorCodes::InterruptedDueToReplStateChange);

        donor.reset();
        stepUp(opCtx.get());

        auto maybeDonor = DonorStateMachine::lookup(opCtx.get(), _service, instanceId);
        ASSERT_TRUE(bool(maybeDonor));

        donor = *maybeDonor;
        doneTransitionGuard.reset();

        notifyReshardingAborting(opCtx.get(), *donor, doc);
        ASSERT_OK(donor->getCompletionFuture().getNoThrow());
        checkStateDocumentRemoved(opCtx.get());

        {
            AutoGetCollection coll(opCtx.get(), doc.getSourceNss(), MODE_IS);
            ASSERT_TRUE(bool(coll));
            ASSERT_EQ(coll->uuid(), doc.getSourceUUID());
        }
    }
}

TEST_F(ReshardingDonorServiceTest, RetainsSourceCollectionOnAbort) {
    for (bool isAlsoRecipient : {false, true}) {
        LOGV2(5641803,
              "Running case",
              "test"_attr = _agent.getTestName(),
              "isAlsoRecipient"_attr = isAlsoRecipient);

        auto doc = makeStateDocument(isAlsoRecipient);
        auto opCtx = makeOperationContext();

        createSourceCollection(opCtx.get(), doc);
        if (isAlsoRecipient) {
            createTemporaryReshardingCollection(opCtx.get(), doc);
        }

        DonorStateMachine::insertStateDocument(opCtx.get(), doc);
        auto donor = DonorStateMachine::getOrCreate(opCtx.get(), _service, doc.toBSON());

        notifyRecipientsDoneCloning(opCtx.get(), *donor, doc);
        notifyToStartBlockingWrites(opCtx.get(), *donor, doc);

        {
            AutoGetCollection coll(opCtx.get(), doc.getSourceNss(), MODE_IS);
            ASSERT_TRUE(bool(coll));
            ASSERT_EQ(coll->uuid(), doc.getSourceUUID());
        }

        notifyReshardingAborting(opCtx.get(), *donor, doc);
        ASSERT_OK(donor->getCompletionFuture().getNoThrow());
        checkStateDocumentRemoved(opCtx.get());

        {
            AutoGetCollection coll(opCtx.get(), doc.getSourceNss(), MODE_IS);
            ASSERT_TRUE(bool(coll));
            ASSERT_EQ(coll->uuid(), doc.getSourceUUID());
        }
    }
}

}  // namespace
}  // namespace mongo
