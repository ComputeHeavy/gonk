import core
import sigs
import memstate
import memrk
import memd
import unittest
import hashlib
import nacl
from nacl import signing

class TestState(unittest.TestCase):
    def standard_object(self):
        fields = {
            "name": "object.txt",
            "format": "text/plain",
            "size": len("object contents"),
            "hash_type": core.HashTypeT.SHA256,
            "hash": hashlib.sha256(b"object contents").hexdigest(),
        }

        return core.Object(fields["name"], fields["format"], fields["size"], 
            fields["hash_type"], fields["hash"])

    def versioned_object(self, object_: core.Object):
        obj = object_.__copy__()
        obj.version += 1
        obj.name = "object.rtf"

        return obj

    def standard_schema(self):
        fields = {
            "name": "schema-sample",
            "format": "application/schema+json",
            "size": len("schema contents"),
            "hash_type": core.HashTypeT.SHA256,
            "hash": hashlib.sha256(b"schema contents").hexdigest(),
        }

        return core.Object(fields["name"], fields["format"], fields["size"], 
            fields["hash_type"], fields["hash"])

    def standard_annotation(self, schema: core.Identifier):
        fields = {
            "size": len("annotation contents"),
            "hash_type": core.HashTypeT.SHA256,
            "hash": hashlib.sha256(b"annotation contents").hexdigest(),
        }

        return core.Annotation(schema, fields["size"], fields["hash_type"], 
            fields["hash"])

    def versioned_annotation(self, annotation: core.Annotation):
        anno = annotation.__copy__()
        anno.version += 1

        return anno

    def test_machine_register(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        self.assertEqual(len(machine.validators), 1)
        self.assertEqual(machine.validators[0], state)
        self.assertEqual(len(machine.consumers), 2)
        self.assertEqual(machine.consumers[0], record_keeper)
        self.assertEqual(machine.consumers[1], state)

    def test_object_create(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 1)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            o1v0.identifier()])

        rae1 = core.ReviewAcceptEvent(oce.uuid)
        rae1 = signer.sign(rae1)
        machine.process_event(rae1)

        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 0)

    def test_object_update(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        o1v1 = self.versioned_object(o1v0)
        oue = signer.sign(core.ObjectUpdateEvent(o1v1))
        machine.process_event(oue)

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 2)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertEqual(state.objects[o1v0.uuid][1], o1v1)
        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            o1v1.identifier()])

        rae1 = core.ReviewAcceptEvent(oue.uuid)
        rae1 = signer.sign(rae1)
        machine.process_event(rae1)

        self.assertEqual(len(state.entity_status[o1v1.identifier()]), 0)

    def test_object_delete(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        ode = signer.sign(core.ObjectDeleteEvent(o1v0.identifier()))
        machine.process_event(ode)

        self.assertEqual(len(state.objects), 1)
        self.assertEqual(len(state.objects[o1v0.uuid]), 1)
        self.assertEqual(state.objects[o1v0.uuid][0], o1v0)
        self.assertTrue(memstate.TagT.DELETE_PENDING in state.entity_status[
            o1v0.identifier()])

        rae1 = core.ReviewAcceptEvent(ode.uuid)
        rae1 = signer.sign(rae1)
        machine.process_event(rae1)

        self.assertTrue(memstate.TagT.DELETE_ACCEPTED in state.entity_status[
            o1v0.identifier()])
        self.assertEqual(len(state.entity_status[o1v0.identifier()]), 2)

    def test_annotation_create(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))
        machine.process_event(sce)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))
        machine.process_event(oce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(
            core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 1)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        
        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            a1v0.identifier()])

        self.assertEqual(len(state.object_annotation_link.forward), 1)
        self.assertEqual(
            len(state.object_annotation_link.forward[o1v0.identifier()]), 1)
        self.assertEqual(state.object_annotation_link.forward[
            o1v0.identifier()][0], a1v0.uuid)

        self.assertEqual(len(state.object_annotation_link.reverse), 1)
        self.assertEqual(
            len(state.object_annotation_link.reverse[a1v0.uuid]), 1)
        self.assertEqual(state.object_annotation_link.reverse[a1v0.uuid][0], 
            o1v0.identifier())

        rae1 = core.ReviewAcceptEvent(ace.uuid)
        rae1 = signer.sign(rae1)
        machine.process_event(rae1)

        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 0)

    def test_annotation_update(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))        
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))        
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))            
        machine.process_event(ace)

        a1v1 = self.versioned_annotation(a1v0)
        aue = signer.sign(core.AnnotationUpdateEvent(a1v1))        
        machine.process_event(aue)

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 2)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        self.assertEqual(state.annotations[a1v0.uuid][1], a1v1)

        self.assertTrue(memstate.TagT.CREATE_PENDING in state.entity_status[
            a1v1.identifier()])

        rae1 = core.ReviewAcceptEvent(aue.uuid)
        rae1 = signer.sign(rae1)
        machine.process_event(rae1)

        self.assertEqual(len(state.entity_status[a1v1.identifier()]), 0)


    def test_annotation_delete(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)

        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(vk1)
        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        o1v0 = self.standard_object()
        oce = signer.sign(core.ObjectCreateEvent(o1v0))            
        machine.process_event(oce)

        s1v0 = self.standard_schema()
        sce = signer.sign(core.ObjectCreateEvent(s1v0))            
        machine.process_event(sce)

        a1v0 = self.standard_annotation(s1v0.identifier())
        ace = signer.sign(core.AnnotationCreateEvent([o1v0.identifier()], a1v0))
        machine.process_event(ace)

        ade = signer.sign(core.AnnotationDeleteEvent(a1v0.identifier()))    
        machine.process_event(ade)

        self.assertEqual(len(state.annotations), 1)
        self.assertEqual(len(state.annotations[a1v0.uuid]), 1)
        self.assertEqual(state.annotations[a1v0.uuid][0], a1v0)
        self.assertTrue(memstate.TagT.DELETE_PENDING in state.entity_status[
            a1v0.identifier()])

        rae1 = core.ReviewAcceptEvent(ade.uuid)
        rae1 = signer.sign(rae1)
        machine.process_event(rae1)

        self.assertTrue(memstate.TagT.DELETE_ACCEPTED in state.entity_status[
            a1v0.identifier()])
        self.assertEqual(len(state.entity_status[a1v0.identifier()]), 2)

    def test_owner_add(self):
        depot = memd.Depot()
        machine = core.Machine()

        record_keeper = memrk.RecordKeeper()
        machine.register(record_keeper)

        state = memstate.State(record_keeper)
        machine.register(state)

        sk1 = nacl.signing.SigningKey.generate()
        signer = sigs.Signer(sk1)
        
        vk1 = sk1.verify_key
        wae1 = core.OwnerAddEvent(vk1)

        wae1 = signer.sign(wae1)
        machine.process_event(wae1)

        self.assertEqual(len(state.owners), 1)

        sk2 = nacl.signing.SigningKey.generate()
        vk2 = sk2.verify_key
        oae2 = core.OwnerAddEvent(vk2)

        oae2 = signer.sign(oae2)
        machine.process_event(oae2)

        self.assertEqual(len(state.owners), 2)

if __name__ == '__main__':
    unittest.main()