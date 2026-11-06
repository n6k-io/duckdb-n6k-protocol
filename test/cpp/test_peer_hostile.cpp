#include "catch.hpp"
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

} // namespace

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
