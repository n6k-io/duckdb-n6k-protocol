#include "arrow_ipc_encode.hpp"
#include "catch.hpp"
#include "frame_codec.hpp"
#include "framework/n6k_peer.hpp"
#include "framework/n6k_twin.hpp"

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace duckdb;
using n6k_test::PackPush;
using n6k_test::SchemaWriter;
using n6k_test::ScriptedPeer;

namespace {

using RowList = std::vector<std::string>;
using Packer = msgpack::packer<msgpack::sbuffer>;

struct PushCase {
	std::string name;
	SchemaWriter schemas;
};

SchemaWriter StringList(std::string value) {
	return [value](Packer &pk) {
		pk.pack_array(1);
		pk.pack(value);
	};
}

const std::string ALIVE = "SELECT alive FROM n6k_connections() ORDER BY alive";

void Serve(TwinNear &near, ScriptedPeer &victim, ScriptedPeer &bystander) {
	near.Attach("victim", near.Connect(victim.ClientFd()));
	near.Attach("bystander", near.Connect(bystander.ClientFd(), ", token => 'other'"));
	REQUIRE(victim.OpenNs() != 0);
}

std::string SchemaIpc() {
	DuckDB db(nullptr);
	Connection con(db);
	n6k::ArrowIpcStreamEncoder encoder(*con.context, vector<LogicalType> {LogicalType::BIGINT}, vector<string> {"x"});
	return encoder.SchemaMessage();
}

struct StreamOp {
	std::string name;
	uint8_t op;
	std::string sql;
	std::string prefix;
};

const std::vector<StreamOp> STREAM_OPS = {
    {"QUERY", n6k::OP_QUERY, "SELECT * FROM n6k_catalog_query('victim', 'SELECT 1')", "n6k[victim] QUERY:"},
    {"RPC_SCALAR", n6k::OP_RPC_SCALAR, "SELECT * FROM n6k_catalog_rpc('victim', 'fn')", "n6k[victim] RPC_SCALAR:"},
    {"RPC_TABLE", n6k::OP_RPC_TABLE, "SELECT * FROM n6k_catalog_rpc_table('victim', 'fn', (SELECT 1 AS id))",
     "n6k[victim] RPC_TABLE:"},
};

using StreamEnding = std::function<void(ScriptedPeer &, const n6k::FrameTarget &)>;

// RPC_TABLE binds with a probe of the same op, which must succeed for the real call to run.
void ScriptStream(ScriptedPeer &peer, const StreamOp &op, const StreamEnding &ending, bool &probed) {
	const auto schema = SchemaIpc();
	peer.OnRequest([&peer, &op, &probed, ending, schema](const n6k_test::PeerRequest &request) {
		if (request.op != op.op) {
			return;
		}
		const n6k::FrameTarget target {request.ns, request.id};
		peer.Send(n6k::PackRespSchema(target, schema));
		if (op.op == n6k::OP_RPC_TABLE && !probed) {
			probed = true;
			peer.Send(n6k::PackRespEndRowcountNull(target));
			return;
		}
		ending(peer, target);
	});
}

} // namespace

TEST_CASE("a stream ends cleanly only on a RESP_END the server did not cancel", "[peer]") {
	for (const auto &op : STREAM_OPS) {
		DYNAMIC_SECTION(op.name) {
			bool probed = false;
			TwinNear near;
			ScriptedPeer peer;
			near.Attach("victim", near.Connect(peer.ClientFd()));
			REQUIRE(peer.OpenNs() != 0);

			SECTION("an ERR after the schema keeps its type") {
				ScriptStream(
				    peer, op,
				    [](ScriptedPeer &p, const n6k::FrameTarget &target) {
					    p.Send(n6k::PackErr(target, "PermissionException", "denied", false));
				    },
				    probed);
				near.Fails(op.sql, ExceptionType::PERMISSION, op.prefix);
				near.Fails(op.sql, ExceptionType::PERMISSION, "denied");
			}

			SECTION("a hangup after the schema is an IO error") {
				ScriptStream(
				    peer, op, [](ScriptedPeer &p, const n6k::FrameTarget &) { p.Hangup(); }, probed);
				near.Fails(op.sql, ExceptionType::IO, op.prefix);
			}

			SECTION("a cancel the client did not ask for is an IO error") {
				ScriptStream(
				    peer, op,
				    [](ScriptedPeer &p, const n6k::FrameTarget &target) { p.Send(n6k::PackRespEndCancelled(target)); },
				    probed);
				near.Fails(op.sql, ExceptionType::IO, "server cancelled the request");
			}

			SECTION("a plain RESP_END is the end of the rows") {
				ScriptStream(
				    peer, op,
				    [](ScriptedPeer &p, const n6k::FrameTarget &target) {
					    p.Send(n6k::PackRespEndRowcountNull(target));
				    },
				    probed);
				REQUIRE(near.Rows(op.sql).empty());
			}
		}
	}
}

TEST_CASE("a bad frame closes only its connection and odd names keep it", "[peer]") {
	const std::vector<PushCase> bad = {
	    {"a map",
	     [](Packer &pk) {
		     pk.pack_map(1);
		     pk.pack(std::string("a"));
		     pk.pack(1);
	     }},
	    {"a mixed array",
	     [](Packer &pk) {
		     pk.pack_array(3);
		     pk.pack(1);
		     pk.pack_nil();
		     pk.pack_map(1);
		     pk.pack(std::string("a"));
		     pk.pack(1);
	     }},
	    {"a string",
	     [](Packer &pk) {
		     pk.pack(std::string("main"));
	     }},
	    {"an integer",
	     [](Packer &pk) {
		     pk.pack(7);
	     }},
	};
	for (const auto &push : bad) {
		DYNAMIC_SECTION("a push whose schemas are " << push.name) {
			TwinNear near;
			ScriptedPeer victim;
			ScriptedPeer bystander;
			Serve(near, victim, bystander);

			victim.Send(PackPush(victim.OpenNs(), push.schemas));

			REQUIRE(victim.AwaitEof());
			REQUIRE(near.Rows(ALIVE) == RowList {"false", "true"});
			REQUIRE(bystander.RoundTrip());
		}
	}

	const std::vector<std::string> odd = {"nope", "", std::string(2000000, 'x'), "a'\n\"\\"};
	for (size_t i = 0; i < odd.size(); i++) {
		DYNAMIC_SECTION("a valid schema name, case " << i) {
			TwinNear near;
			ScriptedPeer victim;
			ScriptedPeer bystander;
			Serve(near, victim, bystander);

			victim.Send(PackPush(victim.OpenNs(), StringList(odd[i])));

			REQUIRE(victim.RoundTrip());
			REQUIRE(near.Rows(ALIVE) == RowList {"true", "true"});
		}
	}

	SECTION("a stream header naming an unknown frame type") {
		TwinNear near;
		ScriptedPeer peer;
		const auto connection_id = near.Connect(peer.ClientFd());

		peer.SendRaw(std::string("\x00\x00\x00\x00\xff", 5));

		REQUIRE(peer.AwaitEof());
		REQUIRE(near.Rows("SELECT alive FROM n6k_connections()") == RowList {"false"});
		near.Rows("SELECT * FROM n6k_disconnect('" + connection_id + "')");
		REQUIRE(near.Rows("SELECT count(*) FROM n6k_connections()") == RowList {"0"});
	}
}
