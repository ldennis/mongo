#include "mongo/platform/basic.h"

#include "mongo/db/fcv_op_observer.h"

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/commands/feature_compatibility_version_parser.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/util/assert_util.h"

namespace mongo {

namespace {
const auto documentIdDecoration = OperationContext::declareDecoration<BSONObj>();
}  // namespace

FcvOpObserver::FcvOpObserver() = default;

FcvOpObserver::~FcvOpObserver() = default;

void FcvOpObserver::onInserts(OperationContext* opCtx,
                              const NamespaceString& nss,
                              OptionalCollectionUUID uuid,
                              std::vector<InsertStatement>::const_iterator first,
                              std::vector<InsertStatement>::const_iterator last,
                              bool fromMigrate) {
    if (nss == NamespaceString::kServerConfigurationNamespace) {
        // We must check server configuration collection writes for featureCompatibilityVersion
        // document changes.
        for (auto it = first; it != last; it++) {
            FeatureCompatibilityVersion::onInsertOrUpdate(opCtx, it->doc);
        }
    }
}

void FcvOpObserver::onUpdate(OperationContext* opCtx, const OplogUpdateEntryArgs& args) {
    if (args.updateArgs.update.isEmpty()) {
        return;
    }
    if (args.nss == NamespaceString::kServerConfigurationNamespace) {
        // We must check server configuration collection writes for featureCompatibilityVersion
        // document changes.
        FeatureCompatibilityVersion::onInsertOrUpdate(opCtx, args.updateArgs.updatedDoc);
    }
}

void FcvOpObserver::onDelete(OperationContext* opCtx,
                             const NamespaceString& nss,
                             OptionalCollectionUUID uuid,
                             StmtId stmtId,
                             bool fromMigrate,
                             const boost::optional<BSONObj>& deletedDoc) {
    if (nss.isServerConfigurationCollection()) {
        auto& documentId = documentIdDecoration(opCtx);
        invariant(!documentId.isEmpty());
        if (documentId.toString() == FeatureCompatibilityVersionParser::kParameterName) {
            uasserted(40670, "removing FeatureCompatibilityVersion document is not allowed");
        }
    }
}

void FcvOpObserver::onReplicationRollback(OperationContext* opCtx,
                                          const RollbackObserverInfo& rbInfo) {
    // Make sure the in-memory FCV matches the on-disk FCV.
    FeatureCompatibilityVersion::onReplicationRollback(opCtx);
}

}  // namespace mongo