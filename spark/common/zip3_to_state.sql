DROP TABLE IF EXISTS zip3_to_state;
CREATE TABLE zip3_to_state (zip3 string, state string);
INSERT INTO zip3_to_state VALUES
    ('012', 'MA'), ('013', 'MA'), ('017', 'MA'), ('023', 'MA'), ('027', 'MA'), ('031', 'NH'), ('036', 'NH'), ('039', 'ME'), ('054', 'VT'), ('058', 'VT'), ('059', 'VT'), ('062', 'CT'), ('068', 'CT'), ('073', 'NJ'), ('084', 'NJ'), ('089', 'NJ'), ('092', 'AE'), ('093', 'AE'), ('096', 'AE'), ('103', 'NY'), ('113', 'NY'), ('116', 'NY'), ('118', 'NY'), ('121', 'NY'), ('127', 'NY'), ('131', 'NY'), ('132', 'NY'), ('135', 'NY'), ('142', 'NY'), ('147', 'NY'), ('149', 'NY'), ('154', 'PA'), ('161', 'PA'), ('180', 'PA'), ('183', 'PA'), ('207', 'MD'), ('208', 'MD'), ('210', 'MD'), ('220', 'VA'), ('227', 'VA'), ('230', 'VA'), ('234', 'VA'), ('242', 'VA'), ('246', 'VA'), ('253', 'WV'), ('254', 'WV'), ('258', 'WV'), ('259', 'WV'), ('260', 'WV'), ('264', 'WV'), ('267', 'WV'), ('271', 'NC'), ('275', 'NC'), ('279', 'NC'), ('280', 'NC'), ('284', 'NC'), ('286', 'NC'), ('303', 'GA'), ('305', 'GA'), ('310', 'GA'), ('314', 'GA'), ('325', 'FL'), ('326', 'FL'), ('327', 'FL'), ('328', 'FL'), ('338', 'FL'), ('341', 'FL'), ('361', 'AL'), ('377', 'TN'), ('388', 'MS'), ('394', 'MS'), ('397', 'MS'), ('402', 'KY'), ('415', 'KY'), ('424', 'KY'), ('445', 'OH'), ('451', 'OH'), ('455', 'OH'), ('469', 'IN'), ('477', 'IN'), ('483', 'MI'), ('485', 'MI'), ('489', 'MI'), ('498', 'MI'), ('503', 'IA'), ('504', 'IA'), ('524', 'IA'), ('528', 'IA'), ('534', 'WI'), ('535', 'WI'), ('539', 'WI'), ('554', 'MN'), ('556', 'MN'), ('567', 'MN'), ('581', 'ND'), ('584', 'ND'), ('591', 'MT'), ('597', 'MT'), ('600', 'IL'), ('601', 'IL'), ('606', 'IL'), ('620', 'IL'), ('624', 'IL'), ('625', 'IL'), ('628', 'IL'), ('634', 'MO'), ('637', 'MO'), ('640', 'MO'), ('658', 'MO'), ('662', 'KS'), ('692', 'NE'), ('693', 'NE'), ('703', 'LA'), ('723', 'AR'), ('724', 'AR'), ('726', 'AR'), ('737', 'OK'), ('739', 'OK'), ('757', 'TX'), ('773', 'TX'), ('779', 'TX'), ('780', 'TX'), ('787', 'TX'), ('791', 'TX'), ('795', 'TX'), ('797', 'TX'), ('798', 'TX'), ('805', 'CO'), ('812', 'CO'), ('828', 'WY'), ('836', 'ID'), ('837', 'ID'), ('850', 'AZ'), ('864', 'AZ'), ('871', 'NM'), ('874', 'NM'), ('878', 'NM'), ('907', 'CA'), ('910', 'CA'), ('911', 'CA'), ('926', 'CA'), ('927', 'CA'), ('931', 'CA'), ('943', 'CA'), ('973', 'OR'), ('974', 'OR'), ('986', 'WA'), ('989', 'WA'), ('996', 'AK'), ('999', 'AK'), ('007', 'PR'), ('032', 'NH'), ('034', 'NH'), ('056', 'VT'), ('071', 'NJ'), ('072', 'NJ'), ('079', 'NJ'), ('081', 'NJ'), ('086', 'NJ'), ('087', 'NJ'), ('091', 'AE'), ('108', 'NY'), ('110', 'NY'), ('111', 'NY'), ('115', 'NY'), ('129', 'NY'), ('137', 'NY'), ('140', 'NY'), ('148', 'NY'), ('160', 'PA'), ('166', 'PA'), ('171', 'PA'), ('175', 'PA'), ('178', 'PA'), ('179', 'PA'), ('182', 'PA'), ('184', 'PA'), ('191', 'PA'), ('192', 'PA'), ('196', 'PA'), ('202', 'DC'), ('205', 'VA'), ('215', 'MD'), ('224', 'VA'), ('228', 'VA'), ('236', 'VA'), ('238', 'VA'), ('252', 'WV'), ('255', 'WV'), ('257', 'WV'), ('262', 'WV'), ('263', 'WV'), ('265', 'WV'), ('289', 'NC'), ('291', 'SC'), ('295', 'SC'), ('297', 'SC'), ('298', 'SC'), ('301', 'GA'), ('312', 'GA'), ('317', 'GA'), ('320', 'FL'), ('324', 'FL'), ('332', 'FL'), ('360', 'AL'), ('362', 'AL'), ('363', 'AL'), ('365', 'AL'), ('366', 'AL'), ('367', 'AL'), ('369', 'AL'), ('378', 'TN'), ('380', 'TN'), ('382', 'TN'), ('384', 'TN'), ('385', 'TN'), ('387', 'MS'), ('403', 'KY'), ('406', 'KY'), ('421', 'KY'), ('425', 'KY'), ('426', 'KY'), ('427', 'KY'), ('437', 'OH'), ('444', 'OH'), ('446', 'OH'), ('456', 'OH'), ('457', 'OH'), ('458', 'OH'), ('461', 'IN'), ('462', 'IN'), ('472', 'IN'), ('473', 'IN'), ('480', 'MI'), ('482', 'MI'), ('488', 'MI'), ('490', 'MI'), ('494', 'MI'), ('496', 'MI'), ('497', 'MI'), ('511', 'IA'), ('516', 'IA'), ('521', 'IA'), ('525', 'IA'), ('548', 'WI'), ('553', 'MN'), ('559', 'MN'), ('569', 'DC'), ('571', 'SD'), ('575', 'SD'), ('576', 'SD'), ('583', 'ND'), ('587', 'ND'), ('588', 'ND'), ('593', 'MT'), ('595', 'MT'), ('596', 'MT'), ('598', 'MT'), ('609', 'IL'), ('610', 'IL'), ('618', 'IL'), ('638', 'MO'), ('644', 'MO'), ('649', 'MO'), ('656', 'MO'), ('665', 'KS'), ('685', 'NE'), ('704', 'LA'), ('706', 'LA'), ('722', 'AR'), ('751', 'TX'), ('754', 'TX'), ('756', 'TX'), ('761', 'TX'), ('765', 'TX'), ('766', 'TX'), ('768', 'TX'), ('769', 'TX'), ('770', 'TX'), ('777', 'TX'), ('778', 'TX'), ('783', 'TX'), ('784', 'TX'), ('788', 'TX'), ('804', 'CO'), ('807', 'CO'), ('810', 'CO'), ('816', 'CO'), ('821', 'WY'), ('826', 'WY'), ('833', 'ID'), ('835', 'ID'), ('865', 'AZ'), ('884', 'NM'), ('891', 'NV'), ('894', 'NV'), ('901', 'CA'), ('903', 'CA'), ('905', 'CA'), ('918', 'CA'), ('922', 'CA'), ('924', 'CA'), ('928', 'CA'), ('933', 'CA'), ('935', 'CA'), ('936', 'CA'), ('938', 'CA'), ('944', 'CA'), ('953', 'CA'), ('954', 'CA'), ('960', 'CA'), ('961', 'CA'), ('959', 'CA'), ('969', 'FM'), ('970', 'OR'), ('971', 'OR'), ('991', 'WA'), ('992', 'WA'), ('005', 'NY'), ('010', 'MA'), ('014', 'MA'), ('028', 'RI'), ('033', 'NH'), ('040', 'ME'), ('046', 'ME'), ('049', 'ME'), ('052', 'VT'), ('053', 'VT'), ('066', 'CT'), ('076', 'NJ'), ('104', 'NY'), ('105', 'NY'), ('112', 'NY'), ('125', 'NY'), ('130', 'NY'), ('157', 'PA'), ('163', 'PA'), ('164', 'PA'), ('165', 'PA'), ('172', 'PA'), ('199', 'DE'), ('201', 'VA'), ('217', 'MD'), ('222', 'VA'), ('225', 'VA'), ('226', 'VA'), ('235', 'VA'), ('240', 'VA'), ('245', 'VA'), ('249', 'WV'), ('250', 'WV'), ('251', 'WV'), ('270', 'NC'), ('273', 'NC'), ('274', 'NC'), ('288', 'NC'), ('299', 'SC'), ('306', 'GA'), ('307', 'GA'), ('308', 'GA'), ('315', 'GA'), ('318', 'GA'), ('322', 'FL'), ('330', 'FL'), ('331', 'FL'), ('333', 'FL'), ('335', 'FL'), ('337', 'FL'), ('340', 'AA'), ('342', 'FL'), ('346', 'FL'), ('349', 'FL'), ('357', 'AL'), ('372', 'TN'), ('381', 'TN'), ('386', 'MS'), ('389', 'MS'), ('398', 'GA'), ('407', 'KY'), ('413', 'KY'), ('414', 'KY'), ('416', 'KY'), ('431', 'OH'), ('435', 'OH'), ('438', 'OH'), ('453', 'OH'), ('454', 'OH'), ('464', 'IN'), ('470', 'IN'), ('474', 'IN'), ('478', 'IN'), ('479', 'IN'), ('499', 'MI'), ('500', 'IA'), ('502', 'IA'), ('507', 'IA'), ('513', 'IA'), ('531', 'WI'), ('550', 'MN'), ('562', 'MN'), ('565', 'MN'), ('592', 'MT'), ('613', 'IL'), ('645', 'MO'), ('660', 'KS'), ('667', 'KS'), ('668', 'KS'), ('678', 'KS'), ('680', 'NE'), ('687', 'NE'), ('690', 'NE'), ('691', 'NE'), ('708', 'LA'), ('710', 'LA'), ('718', 'AR'), ('727', 'AR'), ('740', 'OK'), ('750', 'TX'), ('755', 'TX'), ('758', 'TX'), ('764', 'TX'), ('782', 'TX'), ('789', 'TX'), ('808', 'CO'), ('809', 'CO'), ('823', 'WY'), ('825', 'WY'), ('847', 'UT'), ('853', 'AZ'), ('855', 'AZ'), ('857', 'AZ'), ('870', 'NM'), ('873', 'NM'), ('900', 'CA'), ('913', 'CA'), ('914', 'CA'), ('920', 'CA'), ('921', 'CA'), ('945', 'CA'), ('948', 'CA'), ('955', 'CA'), ('964', 'AP'), ('965', 'AP'), ('966', 'AP'), ('981', 'WA'), ('988', 'WA'), ('990', 'WA'), ('993', 'WA'), ('998', 'AK'), ('006', 'PR'), ('015', 'MA'), ('018', 'MA'), ('020', 'MA'), ('021', 'MA'), ('055', 'MA'), ('060', 'CT'), ('063', 'NY'), ('064', 'CT'), ('065', 'CT'), ('067', 'CT'), ('078', 'NJ'), ('094', 'AE'), ('101', 'NY'), ('114', 'NY'), ('123', 'NY'), ('126', 'NY'), ('134', 'NY'), ('136', 'NY'), ('139', 'NY'), ('155', 'PA'), ('156', 'PA'), ('159', 'PA'), ('162', 'PA'), ('168', 'PA'), ('170', 'PA'), ('177', 'PA'), ('185', 'PA'), ('187', 'PA'), ('203', 'DC'), ('204', 'DC'), ('206', 'MD'), ('211', 'MD'), ('216', 'MD'), ('218', 'MD'), ('219', 'MD'), ('232', 'VA'), ('239', 'VA'), ('241', 'VA'), ('256', 'WV'), ('266', 'WV'), ('276', 'NC'), ('278', 'NC'), ('282', 'NC'), ('287', 'NC'), ('292', 'SC'), ('300', 'GA'), ('302', 'GA'), ('311', 'GA'), ('323', 'FL'), ('344', 'FL'), ('351', 'AL'), ('355', 'AL'), ('368', 'AL'), ('370', 'TN'), ('373', 'TN'), ('375', 'TN'), ('393', 'MS'), ('399', 'GA'), ('408', 'KY'), ('411', 'KY'), ('418', 'KY'), ('436', 'OH'), ('440', 'OH'), ('448', 'OH'), ('449', 'OH'), ('450', 'OH'), ('452', 'OH'), ('467', 'IN'), ('471', 'IN'), ('476', 'IN'), ('501', 'IA'), ('505', 'IA'), ('506', 'IA'), ('509', 'IA'), ('512', 'IA'), ('523', 'IA'), ('526', 'IA'), ('527', 'IA'), ('530', 'WI'), ('541', 'WI'), ('545', 'WI'), ('557', 'MN'), ('560', 'MN'), ('561', 'MN'), ('566', 'MN'), ('590', 'MT'), ('599', 'MT'), ('602', 'IL'), ('603', 'IL'), ('605', 'IL'), ('607', 'IL'), ('611', 'IL'), ('612', 'IL'), ('614', 'IL'), ('615', 'IL'), ('617', 'IL'), ('619', 'IL'), ('623', 'IL'), ('626', 'IL'), ('627', 'IL'), ('630', 'MO'), ('631', 'MO'), ('636', 'MO'), ('641', 'MO'), ('652', 'MO'), ('661', 'KS'), ('666', 'KS'), ('670', 'KS'), ('673', 'KS'), ('681', 'NE'), ('683', 'NE'), ('684', 'NE'), ('730', 'OK'), ('738', 'OK'), ('741', 'OK'), ('743', 'OK'), ('744', 'OK'), ('745', 'OK'), ('747', 'OK'), ('762', 'TX'), ('763', 'TX'), ('767', 'TX'), ('775', 'TX'), ('776', 'TX'), ('799', 'TX'), ('803', 'CO'), ('829', 'WY'), ('830', 'WY'), ('832', 'ID'), ('834', 'ID'), ('840', 'UT'), ('852', 'AZ'), ('856', 'AZ'), ('875', 'NM'), ('879', 'NM'), ('880', 'NM'), ('889', 'NV'), ('895', 'NV'), ('897', 'NV'), ('908', 'CA'), ('915', 'CA'), ('916', 'CA'), ('934', 'CA'), ('940', 'CA'), ('942', 'CA'), ('950', 'CA'), ('951', 'CA'), ('952', 'CA'), ('956', 'CA'), ('957', 'CA'), ('962', 'AP'), ('975', 'OR'), ('976', 'OR'), ('983', 'WA'), ('009', 'PR'), ('011', 'MA'), ('022', 'MA'), ('024', 'MA'), ('026', 'MA'), ('035', 'NH'), ('037', 'NH'), ('038', 'NH'), ('041', 'ME'), ('042', 'ME'), ('044', 'ME'), ('045', 'ME'), ('047', 'ME'), ('061', 'CT'), ('069', 'CT'), ('074', 'NJ'), ('077', 'NJ'), ('080', 'NJ'), ('082', 'NJ'), ('083', 'NJ'), ('085', 'NJ'), ('088', 'NJ'), ('090', 'AE'), ('095', 'AE'), ('098', 'AE'), ('100', 'NY'), ('107', 'NY'), ('109', 'NY'), ('117', 'NY'), ('119', 'NY'), ('120', 'NY'), ('122', 'NY'), ('124', 'NY'), ('141', 'NY'), ('143', 'NY'), ('145', 'NY'), ('146', 'NY'), ('151', 'PA'), ('158', 'PA'), ('167', 'PA'), ('173', 'PA'), ('188', 'PA'), ('189', 'PA'), ('193', 'PA'), ('194', 'PA'), ('195', 'PA'), ('197', 'DE'), ('198', 'DE'), ('200', 'DC'), ('209', 'MD'), ('212', 'MD'), ('214', 'MD'), ('221', 'VA'), ('231', 'VA'), ('233', 'VA'), ('248', 'WV'), ('277', 'NC'), ('281', 'NC'), ('313', 'GA'), ('316', 'GA'), ('321', 'FL'), ('334', 'FL'), ('339', 'FL'), ('347', 'FL'), ('356', 'AL'), ('358', 'AL'), ('376', 'TN'), ('400', 'KY'), ('401', 'KY'), ('405', 'KY'), ('410', 'KY'), ('412', 'KY'), ('417', 'KY'), ('433', 'OH'), ('439', 'OH'), ('459', 'OH'), ('460', 'IN'), ('463', 'IN'), ('466', 'IN'), ('468', 'IN'), ('484', 'MI'), ('486', 'MI'), ('491', 'MI'), ('493', 'MI'), ('514', 'IA'), ('520', 'IA'), ('522', 'IA'), ('537', 'WI'), ('538', 'WI'), ('540', 'WI'), ('543', 'WI'), ('544', 'WI'), ('547', 'WI'), ('549', 'WI'), ('555', 'MN'), ('558', 'MN'), ('570', 'SD'), ('572', 'SD'), ('573', 'SD'), ('574', 'SD'), ('577', 'SD'), ('580', 'ND'), ('586', 'ND'), ('604', 'IL'), ('608', 'IL'), ('622', 'IL'), ('633', 'MO'), ('639', 'MO'), ('648', 'MO'), ('651', 'MO'), ('657', 'MO'), ('664', 'KS'), ('671', 'KS'), ('676', 'KS'), ('677', 'KS'), ('679', 'KS'), ('689', 'NE'), ('701', 'LA'), ('705', 'LA'), ('711', 'LA'), ('713', 'LA'), ('714', 'LA'), ('717', 'AR'), ('719', 'AR'), ('720', 'AR'), ('721', 'AR'), ('725', 'AR'), ('733', 'TX'), ('736', 'OK'), ('749', 'OK'), ('753', 'TX'), ('772', 'TX'), ('774', 'TX'), ('781', 'TX'), ('792', 'TX'), ('794', 'TX'), ('802', 'CO'), ('806', 'CO'), ('813', 'CO'), ('815', 'CO'), ('820', 'WY'), ('822', 'WY'), ('824', 'WY'), ('827', 'WY'), ('831', 'WY'), ('841', 'UT'), ('842', 'UT'), ('851', 'AZ'), ('860', 'AZ'), ('863', 'AZ'), ('885', 'TX'), ('898', 'NV'), ('904', 'CA'), ('912', 'CA'), ('919', 'CA'), ('923', 'CA'), ('932', 'CA'), ('939', 'CA'), ('941', 'CA'), ('946', 'CA'), ('958', 'CA'), ('979', 'OR'), ('994', 'WA'), ('995', 'AK'), ('008', 'VI'), ('016', 'MA'), ('019', 'MA'), ('025', 'MA'), ('029', 'RI'), ('030', 'NH'), ('043', 'ME'), ('048', 'ME'), ('050', 'VT'), ('051', 'VT'), ('057', 'VT'), ('070', 'NJ'), ('075', 'NJ'), ('097', 'AE'), ('102', 'NY'), ('106', 'NY'), ('128', 'NY'), ('133', 'NY'), ('138', 'NY'), ('144', 'NY'), ('150', 'PA'), ('152', 'PA'), ('153', 'PA'), ('169', 'PA'), ('174', 'PA'), ('176', 'PA'), ('181', 'PA'), ('186', 'PA'), ('190', 'PA'), ('205', 'MD'), ('223', 'VA'), ('229', 'VA'), ('237', 'VA'), ('243', 'VA'), ('244', 'VA'), ('247', 'WV'), ('261', 'WV'), ('268', 'WV'), ('272', 'NC'), ('283', 'NC'), ('285', 'NC'), ('290', 'SC'), ('293', 'SC'), ('294', 'SC'), ('296', 'SC'), ('304', 'GA'), ('309', 'GA'), ('319', 'GA'), ('329', 'FL'), ('336', 'FL'), ('350', 'AL'), ('352', 'AL'), ('354', 'AL'), ('359', 'AL'), ('364', 'AL'), ('371', 'TN'), ('374', 'TN'), ('379', 'TN'), ('383', 'TN'), ('390', 'MS'), ('391', 'MS'), ('392', 'MS'), ('395', 'MS'), ('396', 'MS'), ('404', 'KY'), ('409', 'KY'), ('420', 'KY'), ('422', 'KY'), ('423', 'KY'), ('430', 'OH'), ('432', 'OH'), ('434', 'OH'), ('441', 'OH'), ('442', 'OH'), ('443', 'OH'), ('447', 'OH'), ('465', 'IN'), ('475', 'IN'), ('481', 'MI'), ('487', 'MI'), ('492', 'MI'), ('495', 'MI'), ('508', 'IA'), ('510', 'IA'), ('515', 'IA'), ('532', 'WI'), ('542', 'WI'), ('546', 'WI'), ('551', 'MN'), ('563', 'MN'), ('564', 'MN'), ('582', 'ND'), ('585', 'ND'), ('594', 'MT'), ('616', 'IL'), ('629', 'IL'), ('635', 'MO'), ('646', 'MO'), ('647', 'MO'), ('650', 'MO'), ('653', 'MO'), ('654', 'MO'), ('655', 'MO'), ('669', 'KS'), ('672', 'KS'), ('674', 'KS'), ('675', 'KS'), ('686', 'NE'), ('688', 'NE'), ('700', 'LA'), ('707', 'LA'), ('712', 'LA'), ('716', 'AR'), ('728', 'AR'), ('729', 'AR'), ('731', 'OK'), ('734', 'OK'), ('735', 'OK'), ('746', 'OK'), ('748', 'OK'), ('752', 'TX'), ('759', 'TX'), ('760', 'TX'), ('785', 'TX'), ('786', 'TX'), ('790', 'TX'), ('793', 'TX'), ('796', 'TX'), ('800', 'CO'), ('801', 'CO'), ('811', 'CO'), ('814', 'CO'), ('838', 'ID'), ('843', 'UT'), ('844', 'UT'), ('845', 'UT'), ('846', 'UT'), ('859', 'AZ'), ('877', 'NM'), ('881', 'NM'), ('882', 'NM'), ('883', 'NM'), ('890', 'NV'), ('893', 'NV'), ('902', 'CA'), ('906', 'CA'), ('917', 'CA'), ('925', 'CA'), ('930', 'CA'), ('937', 'CA'), ('947', 'CA'), ('949', 'CA'), ('963', 'AP'), ('967', 'HI'), ('968', 'HI'), ('972', 'OR'), ('977', 'OR'), ('978', 'OR'), ('980', 'WA'), ('982', 'WA'), ('984', 'WA'), ('985', 'WA'), ('997', 'AK');

