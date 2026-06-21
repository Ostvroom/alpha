import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import trackers.eth_ws_mint_listener as listener_module
from trackers.eth_ws_mint_listener import (
    ERC1155_TRANSFER_BATCH,
    ERC1155_TRANSFER_SINGLE,
    ERC721_TRANSFER,
    NULL_ADDRESS_TOPIC,
    EthMintListener,
    _decode_erc1155_batch,
)
from trackers.nftscan_live_feed import NftscanLiveFeed


def _word(value: int) -> str:
    return value.to_bytes(32, "big").hex()


def _address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + address.removeprefix("0x")


def _batch_data(ids, values) -> str:
    values_offset = 64 + 32 + (32 * len(ids))
    return (
        "0x"
        + _word(64)
        + _word(values_offset)
        + _word(len(ids))
        + "".join(_word(value) for value in ids)
        + _word(len(values))
        + "".join(_word(value) for value in values)
    )


class _FakeEth:
    block_number = 100

    def __init__(self, logs_by_signature, fail_signature=None):
        self.logs_by_signature = logs_by_signature
        self.fail_signature = fail_signature
        self.filters = []

    def get_logs(self, params):
        topics = params["topics"]
        self.filters.append(topics)
        if topics[0] == self.fail_signature:
            raise RuntimeError("provider rejected filter")
        return self.logs_by_signature.get(topics[0], [])


class _FakeWeb3:
    def __init__(self, logs_by_signature, fail_signature=None):
        self.eth = _FakeEth(logs_by_signature, fail_signature)


class MintListenerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_rpcs = listener_module._HTTP_POLL_RPCS
        listener_module._HTTP_POLL_RPCS = []

    def tearDown(self):
        listener_module._HTTP_POLL_RPCS = self.original_rpcs

    def _logs(self):
        to_address = "0x1111111111111111111111111111111111111111"
        contract = "0x2222222222222222222222222222222222222222"
        tx_hash = "0x" + ("a" * 64)
        batch = _batch_data([1, 2], [3, 4])
        return {
            ERC721_TRANSFER: [{
                "address": contract,
                "topics": [
                    ERC721_TRANSFER,
                    NULL_ADDRESS_TOPIC,
                    _address_topic(to_address),
                    "0x" + _word(9),
                ],
                "data": "0x",
                "transactionHash": tx_hash,
                "blockNumber": 98,
                "logIndex": 1,
            }],
            ERC1155_TRANSFER_SINGLE: [{
                "address": contract,
                "topics": [
                    ERC1155_TRANSFER_SINGLE,
                    _address_topic(to_address),
                    NULL_ADDRESS_TOPIC,
                    _address_topic(to_address),
                ],
                "data": "0x" + _word(7) + _word(4),
                "transactionHash": tx_hash,
                "blockNumber": 98,
                "logIndex": 2,
            }],
            ERC1155_TRANSFER_BATCH: [{
                "address": contract,
                "topics": [
                    ERC1155_TRANSFER_BATCH,
                    _address_topic(to_address),
                    NULL_ADDRESS_TOPIC,
                    _address_topic(to_address),
                ],
                "data": batch,
                "transactionHash": tx_hash,
                "blockNumber": 99,
                "logIndex": 3,
            }],
        }

    def test_batch_decoder(self):
        self.assertEqual(
            _decode_erc1155_batch(_batch_data([1, 2], [3, 4])),
            ([1, 2], [3, 4]),
        )
        self.assertEqual(_decode_erc1155_batch("0xdead"), ([], []))

    async def test_poll_filters_cursor_and_event_amounts(self):
        listener = EthMintListener(max_queue=20)
        fake = _FakeWeb3(self._logs())
        listener._http_w3 = fake

        await listener._http_poll_once()

        self.assertEqual(
            fake.eth.filters,
            [
                [ERC721_TRANSFER, NULL_ADDRESS_TOPIC],
                [ERC1155_TRANSFER_SINGLE, None, NULL_ADDRESS_TOPIC],
                [ERC1155_TRANSFER_BATCH, None, NULL_ADDRESS_TOPIC],
            ],
        )
        self.assertEqual(listener._last_polled_block, 99)
        self.assertEqual(
            [
                (mint["type"], mint.get("amount", 1), mint["log_index"])
                for mint in listener.mints
            ],
            [
                ("erc721", 1, 1),
                ("erc1155", 4, 2),
                ("erc1155_batch", 7, 3),
            ],
        )

        await listener._http_poll_once()
        self.assertEqual(len(fake.eth.filters), 3)

    async def test_cursor_does_not_advance_on_partial_rpc_failure(self):
        listener = EthMintListener(max_queue=20)
        listener._http_w3 = _FakeWeb3(
            self._logs(), fail_signature=ERC1155_TRANSFER_SINGLE
        )

        await listener._http_poll_once()

        self.assertIsNone(listener._last_polled_block)
        self.assertEqual(listener.mints, [])

    async def test_hot_volume_deduplicates_exact_logs_not_transactions(self):
        listener = EthMintListener(max_queue=20)
        listener._http_w3 = _FakeWeb3(self._logs())
        await listener._http_poll_once()

        feed = object.__new__(NftscanLiveFeed)
        feed._mint_tracker = {}
        feed._muted_contracts = set()
        contract = listener.mints[0]["contract_address"]
        for mint in listener.mints:
            feed._record_hot_mint_event(contract, mint)
        feed._record_hot_mint_event(contract, dict(listener.mints[0]))

        self.assertEqual(feed._hot_mint_volume_in_window(contract), 12)
        self.assertEqual(
            len({feed._mint_event_key(mint) for mint in listener.mints}),
            3,
        )


if __name__ == "__main__":
    unittest.main()
