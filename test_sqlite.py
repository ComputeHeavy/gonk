import fs
import core
import uuid
import sigs
import json
import sqlite
import hashlib
import pathlib
import secrets
import unittest
import test_utils

import nacl
from nacl import signing

def rmtree(p):
    for ea in p.iterdir():
        if ea.is_dir():
            rmtree(ea)
        else:
            ea.unlink()
    p.rmdir()

class TestRecordKeeper(unittest.TestCase):
    test_directory: pathlib.Path

    def setUp(self):
        self.closers = []
        self.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        self.test_directory.mkdir()

    def tearDown(self):
        [ea.close() for ea in self.closers]
        test_utils.rmtree(self.test_directory)

    def test_record_keeper_init(self):
        record_keeper = sqlite.RecordKeeper(
            self.test_directory)
        self.closers.append(record_keeper.con)

        db_path = self.test_directory.joinpath("rk.db")
        self.assertTrue(db_path.exists())

    def test_add(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)
        self.closers.append(record_keeper.con)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        record_keeper.add(oae_in)

        cur = record_keeper.con.cursor()
        cur.execute("""SELECT uuid, event FROM events""")
        res = cur.fetchone()
        self.assertTrue(res is not None)

        uuid_, event_json = res
        self.assertEqual(oae_in.uuid, uuid.UUID(uuid_))

        event_data = oae_in.serialize()
        event_data["type"] = oae_in.__class__.__name__
        self.assertEqual(json.dumps(event_data), event_json)

    def test_read(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)
        self.closers.append(record_keeper.con)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae_in = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        record_keeper.add(oae_in)

        oae_out = record_keeper.read(oae_in.uuid)

        self.assertEqual(oae_in, oae_out)

    def test_exists(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)
        self.closers.append(record_keeper.con)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))

        self.assertTrue(not record_keeper.exists(oae1.uuid))

        record_keeper.add(oae1)

        self.assertTrue(record_keeper.exists(oae1.uuid))

    def test_next(self):
        record_keeper = sqlite.RecordKeeper(self.test_directory)
        self.closers.append(record_keeper.con)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        oae1 = signer.sign(core.OwnerAddEvent(bytes(sk1.verify_key)))
        record_keeper.add(oae1)

        sk2 = nacl.signing.SigningKey.generate()
        oae2 = signer.sign(core.OwnerAddEvent(bytes(sk2.verify_key)))
        record_keeper.add(oae2)

        self.assertEqual(record_keeper.next(), oae1.uuid)
        self.assertEqual(record_keeper.next(oae1.uuid), oae2.uuid)
        self.assertEqual(record_keeper.next(oae2.uuid), None)

class TestState(unittest.TestCase):
    def setUp(self):
        self.closers = []
        self.test_directory = pathlib.Path(f"testing-{secrets.token_hex(4)}")
        self.test_directory.mkdir()

    def tearDown(self):
        [ea.close() for ea in self.closers]
        test_utils.rmtree(self.test_directory)

    def standard_object(self):
        return core.Object(
            "object.txt", 
            "text/plain", 
            len("object contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"object contents").hexdigest())

    def versioned_object(self, object_: core.Object):
        obj = object_.__copy__()
        obj.version += 1
        obj.name = "object.rtf"

        return obj

    def standard_schema(self):
        return core.Object(
            "schema-sample", 
            "application/schema+json", 
            len("schema contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"schema contents").hexdigest())

    def standard_annotation(self, schema: core.Identifier):
        return core.Annotation(
            schema, 
            len("annotation contents"), 
            core.HashTypeT.SHA256, 
            hashlib.sha256(b"annotation contents").hexdigest())

    def versioned_annotation(self, annotation: core.Annotation):
        anno = annotation.__copy__()
        anno.version += 1

        return anno

    def test_machine_register(self):
        depot = fs.Depot(self.test_directory)
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        self.closers.append(state.con)
        machine.register(state)

        self.assertEqual(len(machine.validators), 2)
        self.assertEqual(machine.consumers[0], record_keeper)
        self.assertEqual(machine.validators[1], state)
        self.assertEqual(len(machine.consumers), 2)
        self.assertEqual(machine.consumers[0], record_keeper)
        self.assertEqual(machine.consumers[1], state)

    def test_object_create_accept(self):
        depot = fs.Depot(self.test_directory)
        machine = core.Machine()

        record_keeper = fs.RecordKeeper(self.test_directory)
        machine.register(record_keeper)

        state = sqlite.State(self.test_directory, record_keeper)
        self.closers.append(state.con)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = signer.verify_bytes
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        cur = state.con.cursor()
        cur.execute("""SELECT object 
                FROM objects 
                WHERE uuid = ?
                    AND version = ?""", 
            (str(o1v0.uuid), o1v0.version))

        o1v0_json = cur.fetchall()
        self.assertEqual(len(o1v0_json), 1)
        self.assertEqual(len(o1v0_json[0]), 1)

        o1v0_data = json.loads(o1v0_json[0][0])
        o1v0_out = core.Object.deserialize(o1v0_data)

        self.assertEqual(o1v0, o1v0_out)

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])

        self.assertTrue(core.StatusT.CREATE_PENDING in status)

        rae = core.ReviewAcceptEvent(oce.uuid)
        rae = signer.sign(rae)
        machine.process_event(rae)

        cur.execute("""SELECT COUNT(*)
                FROM event_review_link 
                WHERE event_uuid = ?
                    AND review_uuid = ?""", 
            (str(oce.uuid), str(rae.uuid)))

        count, = cur.fetchone()
        self.assertEqual(count, 1)

        cur.execute("""SELECT status
                FROM object_status 
                WHERE uuid = ? 
                    AND version = ?""",
            (str(o1v0.uuid), o1v0.version))

        status = set([getattr(core.StatusT, ea) for ea, in cur.fetchall()])

        self.assertEqual(len(status), 0)

    # def test_object_update_accept(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))
    #     machine.process_event(oce)

    #     o1v1 = self.versioned_object(o1v0)
    #     oue = signer.sign(core.ObjectUpdateEvent(o1v1))
    #     machine.process_event(oue)

    #     self.assertEqual(len(state.object_lookup), 1)
    #     self.assertEqual(len(state.object_lookup[o1v0.uuid]), 2)
    #     self.assertEqual(state.object_lookup[o1v0.uuid][0], o1v0)
    #     self.assertEqual(state.object_lookup[o1v0.uuid][1], o1v1)
    #     self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
    #         o1v1.identifier()])

    #     rae = core.ReviewAcceptEvent(oue.uuid)
    #     rae = signer.sign(rae)
    #     machine.process_event(rae)

    #     self.assertEqual(len(state.entity_status[o1v1.identifier()]), 0)

    # def test_object_delete_accept(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))
    #     machine.process_event(oce)

    #     ode = signer.sign(core.ObjectDeleteEvent(o1v0.identifier()))
    #     machine.process_event(ode)

    #     self.assertEqual(len(state.object_lookup), 1)
    #     self.assertEqual(len(state.object_lookup[o1v0.uuid]), 1)
    #     self.assertEqual(state.object_lookup[o1v0.uuid][0], o1v0)
    #     self.assertTrue(core.StatusT.DELETE_PENDING in state.entity_status[
    #         o1v0.identifier()])

    #     rae = core.ReviewAcceptEvent(ode.uuid)
    #     rae = signer.sign(rae)
    #     machine.process_event(rae)

    #     self.assertTrue(core.StatusT.DELETE_ACCEPTED in state.entity_status[
    #         o1v0.identifier()])
    #     self.assertEqual(len(state.entity_status[o1v0.identifier()]), 2)

    # def test_annotation_create_accept(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     s1v0 = self.standard_schema()
    #     sce = signer.sign(core.ObjectCreateEvent(s1v0))
    #     machine.process_event(sce)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))
    #     machine.process_event(oce)

    #     a1v0 = self.standard_annotation(s1v0.identifier())
    #     ace = signer.sign(
    #         core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
    #     machine.process_event(ace)

    #     self.assertEqual(len(state.annotation_lookup), 1)
    #     self.assertEqual(len(state.annotation_lookup[a1v0.uuid]), 1)
    #     self.assertEqual(state.annotation_lookup[a1v0.uuid][0], a1v0)
        
    #     self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
    #         a1v0.identifier()])

    #     self.assertEqual(len(state.object_annotation_link.forward), 1)
    #     self.assertEqual(
    #         len(state.object_annotation_link.forward[o1v0.identifier()]), 1)
    #     self.assertEqual(state.object_annotation_link.forward[
    #         o1v0.identifier()][0], a1v0.uuid)

    #     self.assertEqual(len(state.object_annotation_link.reverse), 1)
    #     self.assertEqual(
    #         len(state.object_annotation_link.reverse[a1v0.uuid]), 1)
    #     self.assertEqual(state.object_annotation_link.reverse[a1v0.uuid][0], 
    #         o1v0.identifier())

    #     rae = core.ReviewAcceptEvent(ace.uuid)
    #     rae = signer.sign(rae)
    #     machine.process_event(rae)

    #     self.assertEqual(len(state.entity_status[a1v0.identifier()]), 0)

    # def test_annotation_update_accept(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))        
    #     machine.process_event(oce)

    #     s1v0 = self.standard_schema()
    #     sce = signer.sign(core.ObjectCreateEvent(s1v0))        
    #     machine.process_event(sce)

    #     a1v0 = self.standard_annotation(s1v0.identifier())
    #     ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))            
    #     machine.process_event(ace)

    #     a1v1 = self.versioned_annotation(a1v0)
    #     aue = signer.sign(core.AnnotationUpdateEvent(a1v1))        
    #     machine.process_event(aue)

    #     self.assertEqual(len(state.annotation_lookup), 1)
    #     self.assertEqual(len(state.annotation_lookup[a1v0.uuid]), 2)
    #     self.assertEqual(state.annotation_lookup[a1v0.uuid][0], a1v0)
    #     self.assertEqual(state.annotation_lookup[a1v0.uuid][1], a1v1)

    #     self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
    #         a1v1.identifier()])

    #     rae = core.ReviewAcceptEvent(aue.uuid)
    #     rae = signer.sign(rae)
    #     machine.process_event(rae)

    #     self.assertEqual(len(state.entity_status[a1v1.identifier()]), 0)


    # def test_annotation_delete_accept(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))            
    #     machine.process_event(oce)

    #     s1v0 = self.standard_schema()
    #     sce = signer.sign(core.ObjectCreateEvent(s1v0))            
    #     machine.process_event(sce)

    #     a1v0 = self.standard_annotation(s1v0.identifier())
    #     ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
    #     machine.process_event(ace)

    #     ade = signer.sign(core.AnnotationDeleteEvent(a1v0.identifier()))    
    #     machine.process_event(ade)

    #     self.assertEqual(len(state.annotation_lookup), 1)
    #     self.assertEqual(len(state.annotation_lookup[a1v0.uuid]), 1)
    #     self.assertEqual(state.annotation_lookup[a1v0.uuid][0], a1v0)
    #     self.assertTrue(core.StatusT.DELETE_PENDING in state.entity_status[
    #         a1v0.identifier()])

    #     rae = core.ReviewAcceptEvent(ade.uuid)
    #     rae = signer.sign(rae)
    #     machine.process_event(rae)

    #     self.assertTrue(core.StatusT.DELETE_ACCEPTED in state.entity_status[
    #         a1v0.identifier()])
    #     self.assertEqual(len(state.entity_status[a1v0.identifier()]), 2)

    # def test_owner_add(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer1 = sigs.Signer(sk1)
        
    #     vk1 = signer1.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)

    #     wae1 = signer1.sign(wae1)
    #     machine.process_event(wae1)

    #     self.assertEqual(len(state.owner_list), 1)

    #     sk2 = nacl.signing.SigningKey.generate()
    #     signer2 = sigs.Signer(sk2)
    #     oae2 = core.OwnerAddEvent(signer2.verify_bytes)

    #     with self.assertRaises(core.ValidationError):
    #         oae2 = signer2.sign(oae2)
    #         machine.process_event(oae2)    

    #     oae2 = signer1.sign(oae2)
    #     machine.process_event(oae2)

    #     self.assertEqual(len(state.owner_list), 2)

    # def test_owner_remove(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer1 = sigs.Signer(sk1)
        
    #     vk1 = signer1.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)

    #     wae1 = signer1.sign(wae1)
    #     machine.process_event(wae1)

    #     sk2 = nacl.signing.SigningKey.generate()
    #     signer2 = sigs.Signer(sk2)
    #     vk2 = signer2.verify_bytes
    #     oae2 = core.OwnerAddEvent(vk2)

    #     oae2 = signer1.sign(oae2)
    #     machine.process_event(oae2)

    #     with self.assertRaises(core.ValidationError):
    #         ore0 = signer2.sign(core.OwnerRemoveEvent(vk1))
    #         machine.process_event(ore0)

    #     ore1 = signer2.sign(core.OwnerRemoveEvent(vk2))
    #     machine.process_event(ore1)

    #     self.assertEqual(len(state.owner_list), 1)

    #     with self.assertRaises(core.ValidationError):
    #         ore2 = signer1.sign(core.OwnerRemoveEvent(vk1))
    #         machine.process_event(ore2)

    # def test_object_create_reject(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))
    #     machine.process_event(oce)

    #     rre = core.ReviewRejectEvent(oce.uuid)
    #     rre = signer.sign(rre)
    #     machine.process_event(rre)

    #     self.assertEqual(len(state.entity_status[o1v0.identifier()]), 1)
    #     self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
    #         o1v0.identifier()])

    # def test_object_update_reject(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))
    #     machine.process_event(oce)

    #     o1v1 = self.versioned_object(o1v0)
    #     oue = signer.sign(core.ObjectUpdateEvent(o1v1))
    #     machine.process_event(oue)

    #     rre = core.ReviewRejectEvent(oue.uuid)
    #     rre = signer.sign(rre)
    #     machine.process_event(rre)

    #     self.assertEqual(len(state.entity_status[o1v1.identifier()]), 1)
    #     self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
    #         o1v1.identifier()])

    # def test_object_delete_reject(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))
    #     machine.process_event(oce)

    #     ode = signer.sign(core.ObjectDeleteEvent(o1v0.identifier()))
    #     machine.process_event(ode)

    #     rre = core.ReviewRejectEvent(ode.uuid)
    #     rre = signer.sign(rre)
    #     machine.process_event(rre)

    #     self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
    #         o1v0.identifier()])
    #     self.assertEqual(len(state.entity_status[o1v0.identifier()]), 1)

    # def test_annotation_create_reject(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     s1v0 = self.standard_schema()
    #     sce = signer.sign(core.ObjectCreateEvent(s1v0))
    #     machine.process_event(sce)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))
    #     machine.process_event(oce)

    #     a1v0 = self.standard_annotation(s1v0.identifier())
    #     ace = signer.sign(
    #         core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
    #     machine.process_event(ace)

    #     rre = core.ReviewRejectEvent(ace.uuid)
    #     rre = signer.sign(rre)
    #     machine.process_event(rre)

    #     self.assertEqual(len(state.entity_status[a1v0.identifier()]), 1)
    #     self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
    #         a1v0.identifier()])

    # def test_annotation_update_reject(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))        
    #     machine.process_event(oce)

    #     s1v0 = self.standard_schema()
    #     sce = signer.sign(core.ObjectCreateEvent(s1v0))        
    #     machine.process_event(sce)

    #     a1v0 = self.standard_annotation(s1v0.identifier())
    #     ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))            
    #     machine.process_event(ace)

    #     a1v1 = self.versioned_annotation(a1v0)
    #     aue = signer.sign(core.AnnotationUpdateEvent(a1v1))        
    #     machine.process_event(aue)

    #     rre = core.ReviewRejectEvent(aue.uuid)
    #     rre = signer.sign(rre)
    #     machine.process_event(rre)

    #     self.assertEqual(len(state.entity_status[a1v1.identifier()]), 1)
    #     self.assertTrue(core.StatusT.CREATE_REJECTED in state.entity_status[
    #         a1v1.identifier()])

    # def test_annotation_delete_reject(self):
    #     depot = mem.Depot()
    #     machine = core.Machine()

    #     record_keeper = mem.RecordKeeper()
    #     machine.register(record_keeper)

    #     state = mem.State(record_keeper)
    #     state_validator = core.StateValidator(state)
    #     machine.register(state_validator)
    #     state_consumer = mem.StateConsumer(state)
    #     machine.register(state_consumer)

    #     sk1 = nacl.signing.SigningKey.generate()
    #     signer = sigs.Signer(sk1)

    #     vk1 = signer.verify_bytes
    #     wae1 = core.OwnerAddEvent(vk1)
    #     wae1 = signer.sign(wae1)
    #     machine.process_event(wae1)

    #     o1v0 = self.standard_object()
    #     oce = signer.sign(core.ObjectCreateEvent(o1v0))            
    #     machine.process_event(oce)

    #     s1v0 = self.standard_schema()
    #     sce = signer.sign(core.ObjectCreateEvent(s1v0))            
    #     machine.process_event(sce)

    #     a1v0 = self.standard_annotation(s1v0.identifier())
    #     ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
    #     machine.process_event(ace)

    #     ade = signer.sign(core.AnnotationDeleteEvent(a1v0.identifier()))    
    #     machine.process_event(ade)

    #     rre = core.ReviewRejectEvent(ade.uuid)
    #     rre = signer.sign(rre)
    #     machine.process_event(rre)

    #     self.assertTrue(core.StatusT.CREATE_PENDING in state.entity_status[
    #         a1v0.identifier()])
    #     self.assertEqual(len(state.entity_status[a1v0.identifier()]), 1)

if __name__ == '__main__':
    unittest.main()