#include "catch.hpp"
#include "framework/n6k_peer.hpp"
#include "framework/n6k_twin.hpp"

#include <string>

using namespace duckdb;
using n6k_test::ScriptedPeer;

TEST_CASE("the right token reaches the wire", "[peer]") {
	TwinNear near;
	ScriptedPeer peer;
	const std::string scope = ", scope => 'n6k://localhost:8099'";

	SECTION("an inline token") {
		near.Connect(peer.ClientFd(), ", token => 'inline'");
		REQUIRE(peer.Token() == "inline");
	}

	SECTION("a scoped secret resolves") {
		near.Rows("CREATE SECRET s (TYPE n6k, TOKEN 'from-secret', SCOPE 'localhost:8099')");
		near.Connect(peer.ClientFd(), scope);
		REQUIRE(peer.Token() == "from-secret");
	}

	SECTION("a scope no secret matches is anonymous") {
		near.Rows("CREATE SECRET s (TYPE n6k, TOKEN 'from-secret', SCOPE 'otherhost:8099')");
		near.Connect(peer.ClientFd(), scope);
		REQUIRE(peer.Token().empty());
	}

	SECTION("an inline token wins over a scoped secret") {
		near.Rows("CREATE SECRET s (TYPE n6k, TOKEN 'from-secret', SCOPE 'localhost:8099')");
		near.Connect(peer.ClientFd(), ", token => 'inline'" + scope);
		REQUIRE(peer.Token() == "inline");
	}

	SECTION("a global unscoped secret is ignored") {
		near.Rows("CREATE SECRET g (TYPE n6k, TOKEN 'unused')");
		near.Attach("db", near.Connect(peer.ClientFd()));
		REQUIRE(peer.Token().empty());
	}
}
