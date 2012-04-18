register ../../../dist/elephant-bird-1.0.jar;


raw_data = load '/path/to/input_files' using com.twitter.elephantbird.pig.load.ProtobufPigLoader(
	'com.twitter.elephantbird.examples.proto.AddressBookProtos.Person', 
	'com.twitter.elephantbird.examples.proto.ProtobufAddressBookProtosExtensionRegistry')
	/*
	as (
		name: chararray,
		id: int,
		email: chararray,
		phone: bag {
      phone_tuple: tuple (
        number: chararray,
        type: chararray
      )
    },
		ext_info: (
			address: chararray
		),
		ext_ext_info: chararray
	)
	*/
	;

person_phone_numbers = foreach raw_data generate name, 
	FLATTEN(phone.phone_tuple.number) as phone_number, 
	FLATTEN(ext_info.address) as address;

phones_by_person = group person_phone_numbers by name;

person_phone_count_extension = foreach phones_by_person generate group as name, 
	COUNT(person_phone_numbers) as phone_count, 
	FLATTEN(person_phone_numbers.address) as address;
	
dump person_phone_count_extension;